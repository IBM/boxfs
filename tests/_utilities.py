from box_sdk_gen.schemas.file_full import FileFull
from box_sdk_gen.schemas.folder_mini import FolderMini
from collections import defaultdict
import copy
import datetime
import io
import itertools
from typing import IO, Any

import box_sdk_gen
from box_sdk_gen import (
    BoxAPIError,
    File,
    Files,
    FileFull,
    FolderFull,
    FolderMini,
    deserialize,
    BoxClient,
    FileOrFolderScopeScopeField,
)

import pytest


USER_ROOT = {
    "id": "0",
    "etag": "0",
    "type": "folder",
    "sequence_id": None,
    "name": "All Files",
}


def ItemJSON(
    name,
    id,
    created_at,
    modified_at,
    _type="file",
    path_collection=None,
    size=0,
):
    if path_collection is None:
        path = {"total_count": 0, "entries": []}
    else:
        path = {
            "total_count": len(path_collection),
            "entries": path_collection,
        }

    return {
        "id": str(id),
        "etag": "1",
        "type": _type,
        "sequence_id": "3",
        "name": name,
        "sha1": "",
        "file_version": {"id": id, "type": "file_version", "sha1": ""},
        "description": "Test Description",
        "size": size,
        "path_collection": path,
        "created_at": created_at,
        "modified_at": modified_at,
        "content_created_at": created_at,
        "content_modified_at": modified_at,
        "parent": path["entries"][-1] if path["entries"] else None,
        "item_status": "active",
    }


class MockedNetworkClient(box_sdk_gen.NetworkClient):
    def fetch(self, options):
        # For safety, don't actually fetch anything when mocking
        return None


class MockedClient(box_sdk_gen.BoxClient):
    def __init__(self, *args, **kwargs):
        network_session = box_sdk_gen.NetworkSession(
            network_client=MockedNetworkClient(),
            base_urls=box_sdk_gen.BaseUrls(base_url="", upload_url="", oauth_2_url=""),
        )
        super().__init__(auth=None, network_session=network_session)


class MockedCollection(box_sdk_gen.Items):
    def __init__(self, *args, mock_entries=[], **kwargs):
        super().__init__(*args, **kwargs)

        self.mock_entries = mock_entries

    def _load_next_page(self) -> dict:
        if self.mock_entries is None:
            raise box_sdk_gen.BoxAPIError(
                status=404,
                headers=None,
                code="not_found",
                message="Not Found",
                request_id=None,
                url=None,
                method=None,
                context_info={
                    "errors": [
                        {
                            "reason": "invalid_parameter",
                            "name": "folder",
                            "message": "Invalid value. Folder has no mocked items",
                        }
                    ]
                },
                network_response=None,
            )

        response = {
            "entries": self.mock_entries,
            "limit": 1000,
            "offset": 0,
            "total_count": len(self.mock_entries),
        }

        return response


class BoxFileSystemMocker:
    """A template test class with a number of fixtures for mocking Box API and file
    system"""

    _next_id = 1

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, root_path, root_id, scopes):
        # Map file IDs to file contents
        self.contents = {}
        # Map folder IDs to uploaded file lists
        self.mock_items: dict[str, list[FileFull | FolderMini]] = defaultdict(list)
        # Map folder IDs to folder info, start with existing folders
        self.folders: dict[str, dict] = {
            "0": ItemJSON(
                "All Files",
                "0",
                "2000-01-01T00:00:00-08:00",
                "2000-01-01T00:00:00-08:00",
                "folder",
            )
        }
        # Map file IDs to file info
        self.file_items: dict[str, box_sdk_gen.FileFull] = {}

        if not root_path and root_id:
            # Make up a root path to match the ID
            root_path = "Mock Upload Folder"
            self._next_id = int(root_id)

        # Start with ROOT_FOLDER and "Test Upload Folder"
        _folder_id = "0"
        _path_collection = (USER_ROOT,)
        if root_path:
            for folder_name in root_path.split("/"):
                folder = ItemJSON(
                    name=folder_name,
                    id=str(self._next_id),
                    created_at="2000-01-01T00:00:00-08:00",
                    modified_at="2000-01-01T00:00:00-08:00",
                    _type="folder",
                    path_collection=_path_collection,
                )
                self.mock_items[_folder_id].append(deserialize(folder, type=FolderMini))
                self.folders[str(self._next_id)] = folder
                _folder_id = str(self._next_id)
                self._next_id += 1
                _path_collection = _path_collection + (folder,)

    @pytest.fixture(scope="function")
    def fs(self):
        return NotImplementedError("Abstract method")

    @pytest.fixture(scope="function")
    def call_counter(self):
        """Returns a dictionary containing the count of calls to wrapped methods

        Since this is class-scoped, it should only be used to compare at different
        states rather than absolute values"""
        counter = defaultdict(int)
        yield counter

    @pytest.fixture(scope="function")
    def wrap_call(self, do_mock, call_counter):
        """Fixture which returns a function, which yields a monkeypatched
        context when called"""

        def wrap_method(obj, func_name, mocked):
            """Function to be called within each fixture"""
            unmocked = getattr(obj, func_name)
            key = f"{obj.__module__}.{obj.__name__}.{func_name}"

            def wrapped(*args, **kwargs):
                """Function to be hit every time the real/mocked method
                is called"""

                call_counter[key] += 1
                if do_mock:
                    return mocked(*args, **kwargs)
                else:
                    return unmocked(*args, **kwargs)

            with pytest.MonkeyPatch.context() as monkeypatch:
                monkeypatch.setattr(obj, func_name, wrapped)
                yield

        return wrap_method

    @pytest.fixture(scope="function")
    def mock_folder_get_items(test, wrap_call, box_error):
        marker_tracker = itertools.count()
        markers = {}

        def get_items(
            self, folder_id, *, usemarker=None, marker=None, **kwargs
        ) -> list[FileFull | FolderMini]:
            if folder_id in test.mock_items:
                entries = test.mock_items[folder_id]
                next_marker = None
                if len(entries) > 0 and usemarker:
                    if marker:
                        marker_offset = markers[marker]
                    else:
                        marker_offset = 0

                    if marker_offset + 1 < len(entries):
                        next_marker = str(next(marker_tracker))
                        markers[next_marker] = marker_offset + 1
                    else:
                        next_marker = None
                    entries: list[FileFull | FolderMini] = [entries[marker_offset]]
                return box_sdk_gen.Items(entries=entries, next_marker=next_marker)
            elif folder_id in test.folders:
                return box_sdk_gen.Items(entries=[])
            else:
                # Will raise an error when you try to get the list of items
                raise box_error("not_found", object_id=folder_id, _type="folder")

        yield from wrap_call(box_sdk_gen.FoldersManager, "get_folder_items", get_items)

    @pytest.fixture(scope="function")
    def mock_folder_get(test, wrap_call, box_error):
        def get(self, folder_id, **kwargs) -> FolderFull:
            if folder_id in test.folders:
                folder = test.folders[folder_id]
                return deserialize(folder, box_sdk_gen.FolderFull)
            else:
                raise box_error("not_found", _type="folder", object_id=folder_id)

        yield from wrap_call(box_sdk_gen.FoldersManager, "get_folder_by_id", get)

    @pytest.fixture(scope="function")
    def mock_file_get(test, wrap_call, box_error):
        def get(self, file_id, **kwargs) -> FileFull:
            if file_id in test.file_items:
                return test.file_items[file_id]
            else:
                raise box_error("not_found", _type="file", object_id=file_id)

        yield from wrap_call(box_sdk_gen.FilesManager, "get_file_by_id", get)

    @pytest.fixture(scope="function")
    def mock_item_delete(test, do_mock, box_error, scopes):
        def file_delete(self, file_id, **kwargs):
            if scopes and FileOrFolderScopeScopeField.ITEM_DELETE not in scopes:
                raise test.SCOPE_ERROR

            if file_id in test.file_items:
                parent = test.file_items[file_id].parent.id
                found = False
                for i, subitem in enumerate(test.mock_items[parent]):
                    if subitem.id == file_id:
                        found = True
                        break
                if found:
                    test.mock_items[parent].pop(i)
                del test.contents[file_id]
                del test.file_items[file_id]
            else:
                raise box_error("not_found", _type="file", object_id=file_id)

        def folder_delete(self, folder_id, **kwargs):
            if scopes and FileOrFolderScopeScopeField.ITEM_DELETE not in scopes:
                raise test.SCOPE_ERROR

            if folder_id in test.folders:
                parent = test.folders[folder_id]["parent"]["id"]
                found = False
                for i, subitem in enumerate(test.mock_items[parent]):
                    if subitem.id == folder_id:
                        found = True
                        break
                if found:
                    test.mock_items[parent].pop(i)

                del test.folders[folder_id]
            else:
                raise box_error("not_found", _type="folder", object_id=folder_id)

        if do_mock:
            with pytest.MonkeyPatch.context() as monkeypatch:
                monkeypatch.setattr(
                    box_sdk_gen.FilesManager, "delete_file_by_id", file_delete
                )
                monkeypatch.setattr(
                    box_sdk_gen.FoldersManager, "delete_folder_by_id", folder_delete
                )
                yield
        else:
            yield

    @pytest.fixture(scope="function")
    # Needs to be instantiated after create_subfolder, because files should get cleaned
    # up before folders
    def mock_upload(
        test, do_mock, client: BoxClient, fs, scopes, mock_create_subfolder
    ):
        created_files: set[File] = set()
        updated_files: dict[str, box_sdk_gen.FileVersionMini] = dict()

        def upload_file(
            self, attributes: box_sdk_gen.UploadFileAttributes, file, **kwargs
        ) -> Files:
            if scopes and FileOrFolderScopeScopeField.ITEM_UPLOAD not in scopes:
                raise test.SCOPE_ERROR

            data: IO[bytes] = file
            file_id = str(test._next_id)
            test._next_id += 1
            data.seek(0, 0)

            file = _build_file(
                self,
                file_id,
                data.read(),
                parent=attributes.parent.id,
                file_name=attributes.name,
                **kwargs,
            )

            test.mock_items[attributes.parent.id].append(file)
            test.file_items[file_id] = file
            file._parent_id = attributes.parent.id
            created_files.add(file)
            files = Files(total_count=1, entries=[file])
            return files

        def upload_file_version(self, file_id, attributes, file, **kwargs) -> Files:
            if scopes and FileOrFolderScopeScopeField.ITEM_UPLOAD not in scopes:
                raise test.SCOPE_ERROR

            data: IO[bytes] = file
            file_id = file_id
            data.seek(0, 0)
            data_contents = data.read()

            file = _build_file(
                self,
                file_id,
                data_contents,
                file_name=test.file_items[file_id].name,
                **kwargs,
            )

            # Update stored file response object
            for key in test.file_items[file_id].__dict__:
                if hasattr(file, key):
                    setattr(test.file_items[file_id], key, getattr(file, key))
            # test.file_items[file_id]._response_object = file._response_object
            # test.file_items[file_id].__dict__.update(file._response_object)
            files = Files(total_count=1, entries=[file])
            return files

        def _build_file(
            self,
            file_id,
            data_contents,
            parent: str = None,
            file_name: str = None,
            **kwargs,
        ):
            test.contents[file_id] = data_contents
            time = datetime.datetime.now().isoformat(timespec="seconds")
            if parent is None:
                # It's uploading a new version of an existing file
                parent = client.files.get_file_by_id(file_id).parent.id
            path_collection = tuple(test.folders[parent]["path_collection"]["entries"])
            path_collection = path_collection + (test.folders[parent],)

            file = deserialize(
                ItemJSON(
                    name=file_name,
                    id=file_id,
                    created_at=time,
                    modified_at=time,
                    _type="file",
                    path_collection=path_collection,
                    size=len(data_contents),
                ),
                File,
            )
            return file

        if do_mock:
            with pytest.MonkeyPatch.context() as monkeypatch:
                monkeypatch.setattr(
                    box_sdk_gen.UploadsManager, "upload_file", upload_file
                )
                monkeypatch.setattr(
                    box_sdk_gen.UploadsManager,
                    "upload_file_version",
                    upload_file_version,
                )
                yield
            for file in created_files:
                try:
                    file_id = file.id
                    parent_id = file.parent.id
                    del test.file_items[file_id]
                    test.mock_items[parent_id].remove(file)
                except Exception:
                    pass
        else:
            # Modify the upload function to track all uploaded files
            # _original_function = boxsdk.object.folder.Folder.upload_stream
            _original_upload_function = box_sdk_gen.UploadsManager.upload_file
            _original_update_function = box_sdk_gen.UploadsManager.upload_file_version

            def wrap_upload(self, *args, **kwargs):
                file = _original_upload_function(self, *args, **kwargs)
                created_files.add(file.entries[0])
                return file

            def wrap_update(self, file_id, *args, **kwargs):
                original_file_version = client.files.get_file_by_id(
                    file_id
                ).file_version
                files = _original_update_function(self, file_id, *args, **kwargs)
                updated_files[file_id] = original_file_version
                return files

            with pytest.MonkeyPatch.context() as monkeypatch:
                monkeypatch.setattr(
                    box_sdk_gen.UploadsManager, "upload_file", wrap_upload
                )
                monkeypatch.setattr(
                    box_sdk_gen.UploadsManager, "upload_file_version", wrap_update
                )
                yield

            # Delete all the files that were uploaded
            for file in created_files:
                try:
                    client.files.delete_file_by_id(file.id)
                except box_sdk_gen.BoxAPIError as e:
                    # Ok if file not found, for remove file test
                    if e.response_info.status_code != 404:
                        raise e

            # Restore original version of all the files that were updated
            # Note that this will modify the version history:
            #   old_version -> test_updated_version -> new_version (copied from old version)
            for file_id, original_version in updated_files.items():
                try:
                    client.file_versions.promote_file_version(
                        file_id, id=original_version.id, type="file_version"
                    )
                except box_sdk_gen.BoxAPIError as e:
                    # Ok if file not found, for remove file test
                    if e.response_info.status_code != 404:
                        raise e

    @pytest.fixture(scope="function")
    def mock_copy(test, fs, do_mock, setup, client: BoxClient, scopes, box_error):
        created_files: list[File] = []
        updated_files: dict[str, box_sdk_gen.FileVersionMini] = dict()

        def copy(
            self,
            file_id,
            parent: box_sdk_gen.CopyFileParent,
            *,
            name=None,
            version=None,
            **kwargs,
        ):
            if scopes and FileOrFolderScopeScopeField.ITEM_UPLOAD not in scopes:
                raise test.SCOPE_ERROR

            overwriting = False
            new_file_id = None
            original_file_version: box_sdk_gen.FileVersionMini = None
            for item in test.mock_items[parent.id]:
                if item.name == name:
                    raise box_error("item_name_in_use", _type="file", object_id=item.id)
                    # new_file_id = item.id
                    # original_file_version = item.file_version
                    # overwriting = True
                    # break
            if new_file_id is None:
                new_file_id = str(test._next_id)
                test._next_id += 1

            time = datetime.datetime.now().isoformat(timespec="seconds")

            parent_info = test.folders[parent.id]
            path = (
                *parent_info["path_collection"]["entries"],
                {
                    "id": parent.id,
                    "etag": parent_info["etag"],
                    "type": "folder",
                    "sequence_id": parent_info.get("sequence_id", None),
                    "name": parent_info["name"]
                }
            )

            file: File = deserialize(
                ItemJSON(
                    name=name,
                    id=new_file_id,
                    created_at=time,
                    modified_at=time,
                    _type="file",
                    size=len(test.contents[file_id]),
                    path_collection=path
                ),
                File,
            )

            test.contents[new_file_id] = test.contents[file_id]
            test.mock_items[parent.id].append(file)
            test.file_items[new_file_id] = file
            file.parent = client.folders.get_folder_by_id(parent.id)
            if overwriting:
                updated_files[file.id] = original_file_version
            else:
                created_files.append(file)

        if do_mock:
            with pytest.MonkeyPatch.context() as monkeypatch:
                monkeypatch.setattr(box_sdk_gen.FilesManager, "copy_file", copy)
                yield
            for file in created_files:
                try:
                    file_id = file.id
                    parent_id = file.parent.id
                    del test.file_items[file_id]
                    test.mock_items[parent_id].remove(file)
                except Exception:
                    pass
        else:
            _original_function = box_sdk_gen.FilesManager.copy_file

            def wrap(self, *args, **kwargs):
                file = _original_function(self, *args, **kwargs)
                created_files.append(file)
                return file

            with pytest.MonkeyPatch.context() as monkeypatch:
                monkeypatch.setattr(box_sdk_gen.FilesManager, "copy_file", wrap)
                yield
            for file in created_files:
                client.files.delete_file_by_id(file.id)

    @pytest.fixture(scope="function")
    def mock_file_content(test, wrap_call, mock_file_get, box_error):
        def download_file(self, file_id, *args, **kwargs):
            if file_id in test.contents:
                return io.BytesIO(test.contents[file_id])

            raise box_error("not_found", _type="file", object_id=file_id)

        yield from wrap_call(
            box_sdk_gen.DownloadsManager, "download_file", download_file
        )

    @pytest.fixture(scope="function")
    def mock_create_subfolder(test, fs, do_mock, client: BoxClient, scopes):
        created_folders: list[FolderFull] = []

        def create_subfolder(
            self, name, parent: box_sdk_gen.CreateFolderParent, *args, **kwargs
        ):
            if scopes and FileOrFolderScopeScopeField.ITEM_UPLOAD not in scopes:
                raise test.SCOPE_ERROR

            time = datetime.datetime.now().isoformat(timespec="seconds")
            folder_id = str(test._next_id)
            test._next_id += 1
            folder_json = ItemJSON(
                name=name,
                id=folder_id,
                created_at=time,
                modified_at=time,
                _type="folder",
            )
            # Need to create the path collection for the new folder
            path_collection = copy.deepcopy(test.folders[parent.id]["path_collection"])
            path_collection["total_count"] += 1
            path_collection["entries"] = tuple(path_collection["entries"]) + (
                {
                    "id": parent.id,
                    "etag": "0",
                    "type": "folder",
                    "sequence_id": None,
                    "name": test.folders[parent.id]["name"],
                },
            )
            folder_json["path_collection"] = path_collection
            folder_json["parent"] = test.folders[parent.id]
            folder = deserialize(folder_json, FolderFull)
            test.mock_items[parent.id].append(folder)
            test.folders[folder_id] = folder_json
            created_folders.append(folder)
            return folder

        if do_mock:
            with pytest.MonkeyPatch.context() as monkeypatch:
                monkeypatch.setattr(
                    box_sdk_gen.FoldersManager, "create_folder", create_subfolder
                )
                yield
            for folder in reversed(created_folders):
                try:
                    folder_id = folder.id
                    del test.folders[folder_id]
                    if folder.path_collection is not None:
                        parent_id = folder.path_collection.entries[-1].id
                        test.mock_items[parent_id].remove(folder)
                except Exception as e:
                    raise e
        else:
            _original_function = box_sdk_gen.FoldersManager.create_folder

            def wrap(self, *args, **kwargs):
                folder = _original_function(self, *args, **kwargs)
                # Note: mkdir has already checked if the folder exists, so we'll only
                # append to `created_folders` if it's actually new
                created_folders.append(folder)
                return folder

            with pytest.MonkeyPatch.context() as monkeypatch:
                monkeypatch.setattr(box_sdk_gen.FoldersManager, "create_folder", wrap)
                yield
            for folder in reversed(created_folders):
                try:
                    client.folders.delete_folder_by_id(folder.id)
                except box_sdk_gen.BoxAPIError as e:
                    # okay, if parent folder was already deleted
                    if e.response_info.status_code == 404:
                        pass
                    else:
                        raise e

    SCOPE_ERROR = box_sdk_gen.BoxAPIError(
        box_sdk_gen.RequestInfo(
            method="",
            url="",
            query_params={},
            headers={}
        ),
        box_sdk_gen.ResponseInfo(
            status_code=403,
            headers={},
            body="not_found",
            raw_body="not_found",
            code="not_found",
            context_info={
                "errors": [
                    {
                        "reason": "insufficient_scope",
                        "name": "file",
                        "message": "Write permissions not allowed",
                    }
                ]
            },
        ),
        message="Not Found",
    )
