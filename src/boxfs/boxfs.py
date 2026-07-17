"""
boxfs - A fsspec implementation for Box file storage platform
"""

from __future__ import annotations

import contextlib
import hashlib
import logging
import tempfile
from typing import Iterable, Mapping, Optional, Type, Any
import warnings

from box_sdk_gen import (
    BoxClient,
    FilePathCollectionField,
    FolderMini,
    FolderPathCollectionField,
    JWTConfig,
    BoxJWTAuth,
    BoxAPIError,
    BoxDeveloperTokenAuth,
    CreateFolderParent,
    FileFull,
    FolderFull,
    CopyFileParent,
    UploadFileAttributes,
    UploadFileAttributesParentField,
    BoxOAuth,
    FileOrFolderScopeScopeField,
    WebLink,
)

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
_Default = object()

FS_TYPES = {
    "file": "file",
    "folder": "directory",
}


class BoxFileSystem(AbstractFileSystem):
    protocol = "box"
    root_marker = "/"
    root_id = "0"
    _default_root_id = "0"

    default_options = {"refresh": False}

    # fmt: off
    _fields = [
        "id", "type", "name", "size", "modified_at", "created_at", "path_collection",
        "etag", "sequence_id", "sha1", "file_version"
    ]
    # fmt: on

    def __init__(
        self,
        client: Optional[BoxClient] = None,
        oauth: Optional[BoxOAuth | _PathLike] = None,
        client_type: Type[BoxClient] = BoxClient,
        root_id: _ObjectId = None,
        root_path: _PathLike = None,
        path_map: Optional[dict[_PathLike, FileFull]] = None,
        scopes: Optional[Iterable[FileOrFolderScopeScopeField]] = None,
        cache_paths: bool = True,
        **kwargs,
    ):
        """Instantiate BoxFileSystem

        Creates a BoxFileSystem using the box_sdk_gen interface

        Parameters
        ----------
        oauth : BoxOAuth or str, optional
            Box app BoxOAuth or path to configuration file, which is
            passed to `JWTAuth.from_settings_file`, by default None
        client : BoxClient, optional
            Instantiated box_sdk_gen client
        client_type : Type[BoxClient]
            Type of `BoxClient` class to use when connecting to box

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

        path_map : Mapping[path string -> object dict], optional
            Mapping of paths to object dicts, used to populate initial lookup cache
            for quick directory navigation
        scopes : Iterable[FileOrFolderScopeScopeField], optional
            List of permissions to which the API token should be restricted. If None
            (default), no restrictions are applied. If scopes are provided, the client
            connection is (1) downscoped to use only the provided scopes, and
            (2) restricted to the directory/subdirectories of the root folder.
        cache_paths : bool
            Whether to cache paths for quicker directory navigation. May lead to
            unexpected issues when deleting files
        """
        super().__init__(**kwargs)
        if path_map is None:
            path_map = {}
        self.path_map: dict[_PathLike, FileFull] = path_map
        if client is None:
            if isinstance(oauth, str):
                config = JWTConfig.from_config_file(oauth)
                oauth = BoxJWTAuth(config)
            self.connect(oauth, client_type)
        else:
            self.client = client
        self.root_id: _ObjectId = self._get_root_id(root_id, root_path)
        self.root_path: _PathLike = self._get_root_path(self.root_id)

        self._original_client = self.client
        self.scopes = scopes
        if scopes:
            self.downscope_token(self.scopes)

        self._cache = {}
        self.cache_paths = cache_paths

        for option in self.default_options:
            if option in kwargs:
                self.default_options[option] = kwargs[option]

    def connect(self, auth, client_type):
        self.client: BoxClient = client_type(auth)

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
        folder = self.client.folders.get_folder_by_id(
            root_id, fields=["name", "path_collection"]
        )
        return self._construct_path(folder, relative=False)

    def downscope_token(self, scopes: Iterable[FileOrFolderScopeScopeField]):
        """Downscope permissions for the underlying client

        Parameters
        ----------
        scopes : Iterable[box_sdk_gen.FileOrFolderScopeScopeField]
            List of scopes to allow
        """
        url = "".join(
            [
                self.client.network_session.base_urls.base_url,
                "/2.0/folders/",
                str(self.root_id),
            ]
        )
        downscoped_token = self._original_client.auth.downscope_token(
            scopes=scopes,
            resource=url,
        )
        auth = BoxDeveloperTokenAuth(token=downscoped_token.access_token)
        self.client = self._original_client.__class__(auth=auth)
        # The root path changes after downscoping, because the "All Files" folder
        # is hidden
        self.root_path = self._get_root_path(self.root_id)

    def refresh_token(self):
        self._original_client = self._original_client.auth.refresh_token()
        if self.scopes:
            self.downscope_token(self.scopes)

    @classmethod
    def _strip_protocol(cls, path) -> str:
        path = super()._strip_protocol(path)
        path = path.replace("\\", "/")
        # Make all paths start with root marker
        if not path.startswith(cls.root_marker):
            path = cls.root_marker + path
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
            return self.path_map[path].id

        parent = self._parent(path)
        return self.seek_closest_known_path(parent)

    def _get_absolute_path_id(self, path: str):
        try:
            _closest = self.client.folders.get_folder_by_id(
                self._default_root_id, fields=self._fields
            )
        except BoxAPIError as error:
            if error.response_info.status_code == 403:
                raise PermissionError("Could not access user root folder ('All Files')")
            else:
                raise

        _closest_path = _closest.name
        path = self._strip_protocol(path)
        try:
            for part in path.split("/"):
                error = True
                items = self.client.folders.get_folder_items(
                    _closest.id, fields=self._fields
                )
                for item in items.entries:
                    item_path = "/".join((_closest_path, part))
                    if item.type in ("folder", "file") and item.name == part:
                        _closest = item
                        error = False
                        _closest_path = item_path
                        break
                if error:
                    raise FileNotFoundError("Could not find folder in Box Drive")
        except BoxAPIError as error:
            if error.response_info.status_code == 401:
                self.refresh_token()
                return self._get_absolute_path_id(path)
            else:
                raise FileNotFoundError("Could not find folder in Box Drive")

        object_id = _closest.id
        return str(object_id)

    def _get_relative_path_id(self, path: str, root_id=None):
        if root_id is None:
            root_id = self.root_id
        path = self._strip_protocol(path)

        if path in self.path_map:
            return self.path_map[path].id

        _closest_id = self.seek_closest_known_path(path)
        _closest = self.client.folders.get_folder_by_id(_closest_id)
        _closest_path = self._construct_path(_closest)
        remaining_path = path.replace(_closest_path, "", 1)
        if remaining_path == "":
            return _closest_id
        try:
            for part in remaining_path.lstrip("/").split("/"):
                error = True
                items = self.client.folders.get_folder_items(
                    _closest.id, fields=self._fields
                )
                if not items.entries:
                    raise FileNotFoundError("Could not find folder in Box Drive")
                for item in items.entries:
                    item_path = "/".join((_closest_path, item.name))
                    self._add_to_path_map(item_path, item)
                    if item.type in ("folder", "file") and item.name == part:
                        _closest = item
                        error = False
                        _closest_path = item_path
                        break
                if error:
                    raise FileNotFoundError("Could not find folder in Box Drive")
        except BoxAPIError as error:
            if error.response_info.status_code == 401:
                self.refresh_token()
                return self._get_relative_path_id(path)
            else:
                raise FileNotFoundError("Could not find folder in Box Drive")

        object_id = _closest.id

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
            raise FileExistsError(path)
        if not self.exists(parent):
            if create_parents:
                self.mkdir(parent, create_parents=create_parents)
            else:
                raise FileNotFoundError(f"Path `{parent}` does not exist")

        parent_id = self.path_to_file_id(parent)
        self.client.folders.create_folder(
            path.rsplit("/", maxsplit=1)[-1], CreateFolderParent(parent_id)
        )

    def makedirs(self, path, exist_ok=False):
        if self.exists(path):
            if not exist_ok:
                raise FileExistsError(f"Folder at `{path}` already exists")
            else:
                return

        return self.mkdir(path, create_parents=True)

    def _add_to_path_map(self, path, item):
        path = self._get_relative_path(path)
        if self.cache_paths:
            self.path_map[path] = item

    def _remove_from_path_map(self, path):
        path = self._get_relative_path(path)
        self.path_map.pop(path, None)

    def rm(self, path, recursive=False, maxdepth=None, etag=None):
        """Delete files.

        Parameters
        ----------
        path: str or list of str
            File(s) to delete.
        recursive: bool
            If file(s) are directories, recursively delete contents and then
            also remove the directory
        maxdepth: int or None
            Depth to pass to walk for finding files to delete, if recursive.
            If None, there will be no limit and infinite recursion may be
            possible.
        """
        # TODO: Use Box SDK recursive delete if applicable
        path = self.expand_path(path, recursive=recursive, maxdepth=maxdepth)
        for p in reversed(path):
            if self.isdir(p):
                if not recursive:
                    raise ValueError("Cannot delete directory, set recursive=True")
                self.rmdir(p, etag=etag)
            else:
                self.rm_file(p, etag=etag)

    def rm_file(self, path, etag=None):
        """Remove a file. Passes `etag` along to Box delete"""
        file_id = self.path_to_file_id(path)
        self.client.files.delete_file_by_id(file_id, if_match=etag)
        self._remove_from_path_map(path)

    def rmdir(self, path, recursive: bool = False, etag: str | None = None):
        folder_id = self.path_to_file_id(path)
        self.client.folders.delete_folder_by_id(
            folder_id, if_match=etag, recursive=recursive
        )
        self._remove_from_path_map(path)

    def mv(self, path1, path2, recursive=False, maxdepth=None, etag=None):
        # TODO: Use Box SDK to move files via "update"
        super().mv(path1, path2, recursive=recursive, maxdepth=maxdepth)

    def ls(self, path, detail=True, refresh=_Default, **kwargs):
        if refresh is _Default:
            refresh = self.default_options["refresh"]
        path = self._strip_protocol(path)

        object_id = self.path_to_file_id(path)
        cache_path = path.rstrip("/") if path != "/" else path
        items: Optional[list[FileFull | FolderMini | WebLink]] = None
        fsspec_items: list[dict[str, Any]] = []
        _dircached = False

        if not refresh:
            try:
                _from_cache = self._ls_from_cache(cache_path)
                if _from_cache is not None:
                    fsspec_items: list[dict[str, Any]] = _from_cache
                    # Check that the cache didn't return a self folder instead of
                    # the children items, which happens if the parent folder is
                    # cached but the path folder is not
                    did_return_self = (
                        (fsspec_items is not None)
                        and (len(fsspec_items) == 1)
                        and (fsspec_items[0]["name"] == path.rstrip("/"))
                        and (fsspec_items[0]["type"] == "directory")
                    )
                    _dircached = (fsspec_items is not None) and not did_return_self
            except FileNotFoundError:
                # Not in cache, so try to retrieve normally
                pass

        if not _dircached:
            marker = None
            items = []
            try:
                while True:
                    folder_items = self.client.folders.get_folder_items(
                        object_id, fields=self._fields, marker=marker, usemarker=True
                    )
                    if folder_items.entries:
                        items.extend(folder_items.entries)
                    marker = folder_items.next_marker
                    if marker is None or marker == "null" or marker == "":
                        break
            except BoxAPIError as error:
                if error.response_info.status_code == 401:
                    self.refresh_token()
                    return self.ls(path, detail=detail)
                elif error.response_info.status_code in (403, 404, 405):
                    # item is a file, not a folder
                    items = [self.client.files.get_file_by_id(object_id, fields=self._fields)]
                else:
                    raise error

            # Need to convert Box API response to fsspec response dictionary
            fsspec_items = []
            for item in items:
                if isinstance(item, WebLink):
                    continue
                item_path = self._construct_path(item, relative=True)
                fsspec_items.append(
                    {
                        "name": item_path,
                        "size": item.size,
                        "type": FS_TYPES[item.type],
                        "id": item.id,
                        "modified_at": item.modified_at,
                        "created_at": item.created_at,
                        "etag": item.etag,
                    }
                )
                self._add_to_path_map(item_path, item)

            self.dircache[cache_path] = fsspec_items

        if not detail:
            return [item["name"] for item in fsspec_items]
        else:
            return fsspec_items

    def cp_file(self, path1, path2, **kwargs):
        src_id = self.path_to_file_id(path1)
        dest_folder_id = self.path_to_file_id(self._parent(path2))
        version = kwargs.pop("version", None)

        if self.isdir(path1):
            self.mkdirs(path2, exist_ok=True)
        else:
            if self.exists(path2):
                # Don't delete then rewrite, since Box might choose to remove version
                # history if file gets deleted
                raise FileExistsError(f"File at `{path2}` already exists")

            self.client.files.copy_file(
                src_id,
                CopyFileParent(id=dest_folder_id),
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
        return self.client.downloads.get_download_file_url(file_id)

    def _construct_path(self, item: FileFull | FolderMini, relative=True):
        if not hasattr(item, "path_collection"):
            if item.type == "folder":
                item = self.client.folders.get_folder_by_id(item.id, fields=["name", "path_collection"])
            else:
                item = self.client.files.get_file_by_id(item.id, fields=["name", "path_collection"])
            path_collection = item.path_collection
        else:
            path_collection = getattr(item, "path_collection")

        path_parts = []
        path_entries: list[FolderMini]
        # Seems like a bug in box_sdk_gen, where getting folder items with "fields"
        # doesn't deserialize into the appropriate types, instead returning a dict
        if hasattr(path_collection, "entries"):
            path_entries = getattr(path_collection, "entries", [])
        else:
            path_entries = path_collection.get("entries")

        for path_part in path_entries:
            name = getattr(path_part, "name", None) or path_part.get("name")
            path_parts.append(name)
        path = "/".join((*path_parts, item.name))

        if relative:
            path = self._get_relative_path(path)

        return path

    def _open(self, *args, **kwargs):
        return BoxFile(self, *args, **kwargs)

    @contextlib.contextmanager
    def option_context(self, *args, **kwargs):
        original_kwargs = {}
        for kw, new_value in kwargs.items():
            if kw not in self.default_options:
                warnings.warn(f"No option for context option `{kw}`")
                continue
            original_kwargs[kw] = self.default_options[kw]
            self.default_options[kw] = new_value
        yield
        for kw, old_value in original_kwargs.items():
            self.default_options[kw] = old_value


class BoxFile(AbstractBufferedFile):
    fs: BoxFileSystem

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
            cache_type=cache_type,
            cache_options=cache_options,
            size=size,
            **kwargs,
        )
        self.exists = False
        self.etag = None

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

        if self.exists:
            self.etag = self.details["etag"]

    @property
    def details(self):
        if self._details is None:
            self._details = self.fs.info(self.path)
        return self._details

    def close(self):
        # Writeable needs to checked called before super().close()
        _writable = self.writable()
        super().close()
        if _writable:
            self._upload_full_file()
            self._temp_file.close()

    def _initiate_upload(self):
        # Don't actually initiate the Box upload, we need the full file size for that
        # Instead, create a temp file and start writing to it
        self._temp_file = tempfile.SpooledTemporaryFile(self.blocksize * 10)
        self._sha1 = hashlib.sha1()

    def _upload_full_file(self, exist_ok=True):
        if self.exists and not exist_ok:
            raise FileExistsError(
                "File already exists. Specify `exist_ok=True` to overwrite"
            )

        self._temp_file.seek(0)
        if self.offset > self.blocksize * 10:
            # chunked upload
            uploaded_file = self.fs.client.chunked_uploads.upload_big_file(
                file=self._temp_file,
                parent_folder_id=self.folder_id,
                # TODO: Need to double check that this is the file size
                file_size=self.offset,
                file_name=self.name,
            )
        else:
            file_attributes = UploadFileAttributes(
                name=self.name,
                parent=UploadFileAttributesParentField(self.folder_id),
            )
            if not self.exists:
                upload_response = self.fs.client.uploads.upload_file(
                    file_attributes,
                    self._temp_file,
                    content_md_5=self._sha1.hexdigest(),
                )
            else:
                upload_response = self.fs.client.uploads.upload_file_version(
                    self.file_id,
                    file_attributes,
                    self._temp_file,
                    content_md_5=self._sha1.hexdigest(),
                    if_match=self.etag,
                )
            uploaded_file = upload_response.entries[0]
        logger.info(
            f'File "{uploaded_file.name}" uploaded to Box with file ID '
            f"{uploaded_file.id}"
        )

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
        range = None
        if start is not None or end is not None:
            range = f"bytes={start}-{end}"
        return self.fs.client.downloads.download_file(self.file_id, range=range).read()
