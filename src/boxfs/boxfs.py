"""
boxfs - A fsspec implementation for Box file storage platform
"""
from __future__ import annotations

import hashlib
import logging
import tempfile
from typing import (
    Iterable,
    Mapping,
    Optional,
    Type
)

from boxsdk import BoxAPIException, Client, OAuth2
from boxsdk.auth.oauth2 import TokenScope
from boxsdk.object.item import Item
from fsspec.spec import AbstractBufferedFile, AbstractFileSystem

try:
    import boxfs._upath  # noqa: F401
except ModuleNotFoundError:
    # Optional dependency not found
    pass


__all__ = ["BoxFileSystem"]

logger = logging.getLogger(__name__)

_PathLike = str
_ObjectId = str

FS_TYPES = {
    "file": "file",
    "folder": "directory",
}


class BoxFileSystem(AbstractFileSystem):
    protocol = "box"
    root_marker = ""
    root_id = "0"
    _default_root_id = "0"

    # fmt: off
    _fields = [
        "id", "type", "name", "size", "modified_at", "created_at", "path_collection",
        "etag", "sequence_id", "sha1", "file_version"
    ]
    # fmt: on

    def __init__(
        self,
        client: Optional[Client] = None,
        oauth: Optional[OAuth2] = None,
        client_type: Type[Client] = Client,
        root_id: _ObjectId = None,
        root_path: _PathLike = None,
        path_map: Optional[Mapping[_PathLike, _ObjectId]] = None,
        scopes: Optional[Iterable[TokenScope]] = None,
        **kwargs,
    ):
        """Instantiate BoxFileSystem

        Creates a BoxFileSystem using the boxsdk interface

        Parameters
        ----------
        oauth : OAuth2, optional
            Box app OAuth2 configuration, e.g. loaded from `JWTAuth.from_settings_file`,
            by default None
        client : Client, optional
            Instantiated boxsdk client
        client_type : Type[Client]
            Type of `Client` class to use when connecting to box

        If `client` is provided, it is used for handling API calls. Otherwise, the file
        system to instantiate a new client connection, of type `client_type`, using the
        provided `oauth` configuration.

        root_id : Object ID string, optional
            Box ID of folder where file system root is placed, by default None
        root_path : path string, optional
            Path to Box root folder, must be relative to token root (e.g. "All Files").
            The client must have access to the application user's root folder (i.e., it
            cannot be downscoped to a subfolder)
        
        If only `root_id` is provided, the `root_path` is determined from API calls. If 
        only `root_path` is provided, the `root_id` is determined from API calls. If
        neither is provided, the application user's root folder is used.
            
        path_map : Mapping[path string -> object ID string], optional
            Mapping of paths to object ID strings, used to populate initial lookup cache
            for quick directory navigation
        scopes : Iterable[TokenScope], optional
            List of permissions to which the API token should be restricted. If None
            (default), no restrictions are applied. If scopes are provided, the client
            connection is (1) downscoped to use only the provided scopes, and
            (2) restricted to the directory/subdirectories of the root folder.
        """
        super().__init__(**kwargs)
        if path_map is None:
            path_map = {}
        self.path_map = path_map
        if client is None:
            self.connect(oauth, client_type)
        else:
            self.client = client.clone()
        self.root_id = self._get_root_id(root_id, root_path)
        self.root_path = self._get_root_path(self.root_id)

        self._original_client = self.client
        self.scopes = scopes
        if scopes:
            self.downscope_token(self.scopes)

        self._cache = {}

    def connect(self, config, client_type):
        self.client: Client = client_type(config)

    def _get_root_id(self, root_id: _ObjectId = None, root_path: _PathLike = None):
        """Gets the root folder ID

        If root_id is not None, it is returned. Otherwise, if root path is not None, the
        ID of the corresponding folder is determined. If both are None, return the
        default root id of "0"

        Parameters
        ----------
        root_id : _ObjectId, optional
            Root ID if provided, by default None
        root_path : _PathLike, optional
            Root Path if provided, by default None
        """
        if root_id is not None:
            return root_id
        else:
            if root_path is not None:
                root_id = self._get_absolute_path_id(root_path)
            else:
                root_id = self.root_id

        return root_id
    
    def _get_root_path(self, root_id):
        folder = self.client.folder(root_id).get(fields=["name", "path_collection"])
        return self._construct_path(folder, relative=False)

    def downscope_token(self, scopes: Iterable[TokenScope]):
        """Downscope permissions for the underlying client

        Parameters
        ----------
        scopes : Iterable[boxsdk.auth.oath2.TokenScope]
            List of scopes to allow
        """
        downscoped_token = self._original_client.downscope_token(
            scopes=scopes,
            item=self._original_client.folder(self.root_id),
        )
        self.client = self._original_client.__class__(
            oauth=OAuth2(
                client_id=None,
                client_secret=None,
                access_token=downscoped_token.access_token,
            )
        )
        # The root path changes after downscoping, because the "All Files" folder
        # is hidden
        self.root_path = self._get_root_path(self.root_id)

    def refresh_token(self):
        self._original_client = self._original_client.auth.refresh(
            self._original_client.auth.access_token
        )
        if self.scopes:
            self.downscope_token(self.scopes)

    @classmethod
    def _strip_protocol(cls, path) -> str:
        path = super()._strip_protocol(path)
        path = path.replace("\\", "/")
        return path

    def _get_relative_path(self, path: str):
        path = self._strip_protocol(path)
        path = self.root_marker + path.replace(self.root_path, "").lstrip("/")
        return path

    def path_to_file_id(self, path):
        path = self._get_relative_path(path)
        return self._get_relative_path_id(path)

    def seek_closest_known_path(self, path: str) -> _ObjectId:
        """Traverse up the path, looking for a known folder ID"""
        if path == self.root_marker:
            return self.root_id
        if path in self.path_map:
            return self.path_map[path]

        parent = self._parent(path)
        return self.seek_closest_known_path(parent)

    def _get_absolute_path_id(self, path: str):
        _closest = self.client.folder(self._default_root_id)

        try:
            _closest = _closest.get(fields=self._fields)
        except BoxAPIException as error:
            if error.status == 403:
                raise PermissionError("Could not access user root folder ('All Files')")
            else:
                raise

        _closest_path = _closest.name
        path = self._strip_protocol(path)
        try:
            for part in path.split("/"):
                error = True
                items = _closest.get_items(fields=self._fields)
                for item in items:
                    item_path = "/".join((_closest_path, part))
                    if item.type in ("folder", "file") and item.name == part:
                        _closest = item
                        error = False
                        _closest_path = item_path
                        break
                if error:
                    raise FileNotFoundError("Could not find folder in Box Drive")
        except BoxAPIException as error:
            if error.status == 401:
                self.refresh()
                return self._get_absolute_path_id(path)
            else:
                raise FileNotFoundError("Could not find folder in Box Drive")

        object_id = _closest.object_id
        return str(object_id)

    def _get_relative_path_id(self, path: str, root_id=None):
        if root_id is None:
            root_id = self.root_id
        path = self._strip_protocol(path)

        if path in self.path_map:
            return self.path_map[path]

        _closest_id = self.seek_closest_known_path(path)
        _closest = self.client.folder(_closest_id)
        _closest_path = self._construct_path(_closest)
        remaining_path = path.lstrip(self.root_marker).replace(_closest_path, "", 1)
        if remaining_path == "":
            return _closest_id
        try:
            for part in remaining_path.lstrip("/").split("/"):
                error = True
                items = _closest.get_items(fields=self._fields)
                for item in items:
                    item_path = "/".join((_closest_path, part))
                    self.path_map[item_path] = item.id
                    if item.type in ("folder", "file") and item.name == part:
                        _closest = item
                        error = False
                        _closest_path = item_path
                        break
                if error:
                    raise FileNotFoundError("Could not find folder in Box Drive")
        except BoxAPIException as error:
            if error.status == 401:
                self.refresh()
                return self._get_relative_path_id(path)
            else:
                raise FileNotFoundError("Could not find folder in Box Drive")

        object_id = _closest.object_id

        return str(object_id)

    def exists(self, path, **kwargs):
        try:
            self.path_to_file_id(path)
        except FileNotFoundError:
            return False
        else:
            return True

    def mkdir(self, path, create_parents=True, **kwargs):
        path = self._strip_protocol(path)
        parent = self._parent(path)
        if self.exists(path):
            return
        if not self.exists(parent):
            if create_parents:
                self.mkdir(parent, create_parents=create_parents)
            else:
                raise FileNotFoundError(f"Path `{parent}` does not exist")

        parent_id = self.path_to_file_id(parent)
        self.client.folder(parent_id).create_subfolder(path.rsplit("/", maxsplit=1)[-1])

    def makedirs(self, path, exist_ok=False):
        if self.exists(path):
            if not exist_ok:
                raise FileExistsError(f"Folder at `{path}` already exists")
            else:
                return

        return self.mkdir(path, create_parents=True)

    def rm_file(self, path, etag=None):
        """Remove a file. Passes `etag` along to Box delete"""
        file_id = self.path_to_file_id(path)
        self.client.file(file_id).delete(etag=etag)

    def rmdir(self, path, recursive: bool = True, etag: str | None = None):
        folder_id = self.path_to_file_id(path)
        self.client.folder(folder_id).delete(etag=etag)

    def ls(self, path, detail=True, refresh=True, **kwargs):
        path = self._strip_protocol(path)

        object_id = self.path_to_file_id(path)
        cache_path = path.rstrip("/")

        if not refresh:
            items = self._ls_from_cache(cache_path)
            _type = "folder"

        if refresh or not items:
            try:
                _object = self.client.folder(object_id).get()
                _type = _object.type
                items = self.client.folder(object_id).get_items(fields=self._fields)
            except BoxAPIException as error:
                if error.status == 401:
                    self.refresh()
                    return self.ls(path, detail=detail)

                _type = "file"

        if _type == "file":
            # item is a file, not a folder
            items = [self.client.file(object_id).get(fields=self._fields)]
        else:
            items = list(items)
            self.dircache[cache_path] = items

        fs_items = []
        if not detail:
            for item in items:
                item_path = self._construct_path(item, relative=True)
                self.path_map[item_path] = item.id
                fs_items.append(item_path)
        else:
            for item in items:
                item_path = self._construct_path(item, relative=True)
                self.path_map[item_path] = item.id
                fs_items.append(
                    {
                        "name": item_path,
                        "size": item.size,
                        "type": FS_TYPES[item.type],
                        "id": item.id,
                        "modified_at": item.modified_at,
                        "created_at": item.created_at,
                    }
                )

        return fs_items

    def cp_file(self, path1, path2, **kwargs):
        src_id = self.path_to_file_id(path1)
        dest_folder_id = self.path_to_file_id(self._parent(path2))
        version = kwargs.pop("version", None)

        if self.exists(path2):
            # Don't delete then rewrite, since Box might choose to remove version
            # history if file gets deleted
            raise FileExistsError(f"File at `{path2}` already exists")

        self.client.file(src_id).copy(
            parent_folder=self.client.folder(dest_folder_id),
            name=path2.rsplit("/", maxsplit=1)[-1],
            version=version,
        )

    def touch(self, path, truncate=False, **kwargs):
        # Don't truncate by default
        super().touch(path, truncate=truncate, **kwargs)

    def created(self, path):
        import datetime

        info = self.info(path)
        return datetime.datetime.fromisoformat(info["created_at"])

    def modified(self, path):
        import datetime

        info = self.info(path)
        return datetime.datetime.fromisoformat(info["modified_at"])

    def sign(self, path, expiration=100, **kwargs):
        file_id = self.path_to_file_id(path)
        return self.client.file(file_id).get_download_url()

    def _construct_path(self, item: Item, relative=True):
        if not hasattr(item, "path_collection"):
            item = item.get(fields=["name", "path_collection"])
        path_parts = []
        for path_part in item.path_collection["entries"]:
            path_parts.append(path_part["name"])
        path = "/".join((*path_parts, item.name))

        if relative:
            path = self._get_relative_path(path)

        return path

    def _open(self, *args, **kwargs):
        return BoxFile(self, *args, **kwargs)


class BoxFile(AbstractBufferedFile):
    def __init__(
        self,
        fs: BoxFileSystem,
        path,
        mode="rb",
        block_size="default",
        autocommit=True,
        cache_type="readahead",
        cache_options=None,
        size=None,
        **kwargs,
    ):
        super().__init__(
            fs,
            fs._get_relative_path(path),
            mode,
            block_size,
            autocommit=autocommit,
            **kwargs
        )
        self.exists = False

        if self.writable():
            self.location = None
            self._folder_path = fs._parent(path)
            self.name = path.rsplit("/", maxsplit=1)[-1]
            self.folder_id = fs.path_to_file_id(self._folder_path)
            self.exists = fs.exists(path)
            if self.exists:
                if "id" in self.details:
                    self.file_id = self.details["id"]
                else:
                    self.file_id = fs.path_to_file_id(path)
        else:
            if "id" in self.details:
                self.file_id = self.details["id"]
            else:
                self.file_id = fs.path_to_file_id(path)
            self.exists = True

    def close(self):
        # Writeable needs to checked called before super().close()
        _writable = self.writable()
        super().close()
        if _writable:
            self._upload_full_file()
            self._temp_file.close()

    def _upload_full_file(self, exist_ok=True):
        if self.exists and not exist_ok:
            raise FileExistsError(
                "File already exists. Specify `exist_ok=True` to overwrite"
            )

        if not self.exists:
            _object = self.fs.client.folder(self.folder_id)
        else:
            _object = self.fs.client.file(self.file_id)

        if self.offset > self.blocksize * 10:
            # force to disk
            self._temp_file.rollover()
            # chunked upload
            uploader = _object.get_chunked_uploader(
                file_path=self._temp_file.name, file_name=self.name
            )
            uploaded_file = uploader.start()
        else:
            if not self.exists:
                upload = _object.upload_stream
            else:
                upload = _object.update_contents_with_stream

            self._temp_file._file.seek(0)
            uploaded_file = upload(
                file_stream=self._temp_file._file,
                file_name=self.name,
                sha1=self._sha1.hexdigest(),
            )
        logger.info(
            f'File "{uploaded_file.name}" uploaded to Box with file ID '
            f'{uploaded_file.id}'
        )

    def _initiate_upload(self):
        # Don't actually initiate the Box upload, we need the full file size for that
        # Instead, create a temp file and start writing to it
        self._temp_file = tempfile.SpooledTemporaryFile(self.blocksize * 10)
        self._sha1 = hashlib.sha1()

    def _upload_chunk(self, final=False):
        """
        Upload a part of the file to Box.
        """
        # A new self.buffer is created for each chunk
        self.buffer.seek(0)
        data = self.buffer.getvalue()
        self._sha1.update(data)
        self._temp_file.write(data)

    def _fetch_range(self, start, end):
        kwargs = {}
        if start is not None or end is not None:
            kwargs["byte_range"] = (start, end)
        return self.fs.client.file(self.file_id).content(**kwargs)
