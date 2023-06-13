from collections import defaultdict
import copy
import datetime
from typing import IO

import boxsdk
from boxsdk.auth.oauth2 import TokenScope
import boxsdk.object.file
import boxsdk.object.folder
import boxsdk.object.search
from boxsdk.pagination.limit_offset_based_object_collection import (
    LimitOffsetBasedObjectCollection,
)
from boxsdk.session.session import Session
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


class MockedClient(boxsdk.DevelopmentClient):
    def __init__(self, *args, **kwargs):
        super(boxsdk.Client, self).__init__()
        self._oauth = None
        self._session = Session()


class MockedCollection(LimitOffsetBasedObjectCollection):
    def __init__(self, *args, mock_entries=[], **kwargs):
        super().__init__(*args, **kwargs)

        self.mock_entries = mock_entries

    def _load_next_page(self) -> dict:
        if self.mock_entries is None:
            raise boxsdk.BoxAPIException(
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

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, root_path, root_id, scopes):
        # Map file IDs to file contents
        self.contents = {}
        # Map folder IDs to uploaded file lists
        self.mock_items = defaultdict(list)
        # Map folder IDs to folder info, start with existing folders
        self.folders = {
            "0": ItemJSON(
                "All Files",
                "0",
                "2000-01-01T00:00:00-08:00",
                "2000-01-01T00:00:00-08:00",
                "folder",
            )
        }
        # Map file IDs to file info
        self.file_items = {}

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
                self.mock_items[_folder_id].append(
                    boxsdk.object.folder.Folder(
                        session=None,
                        object_id=str(self._next_id),
                        response_object=folder
                    )
                )
                self.folders[str(self._next_id)] = folder
                _folder_id = str(self._next_id)
                self._next_id += 1
                _path_collection = _path_collection + (folder,)

    @pytest.fixture(scope="class")
    def fs(self):
        return NotImplementedError("Abstract method")

    @pytest.fixture(scope="class")
    def mock_folder_get_items(test, do_mock):
        def get_items(self, *args, **kwargs):
            if self.object_id in test.mock_items:
                return MockedCollection(
                    session=Session(),
                    url=None,
                    mock_entries=test.mock_items[self.object_id],
                )
            elif self.object_id in test.folders:
                return MockedCollection(
                    session=Session(),
                    url=None,
                    mock_entries=[],
                )
            else:
                return MockedCollection(
                    session=Session(),
                    url=None,
                    # Will raise an error when you try to get the list of items
                    mock_entries=None,
                )

        if do_mock:
            with pytest.MonkeyPatch.context() as monkeypatch:
                monkeypatch.setattr(boxsdk.object.folder.Folder, "get_items", get_items)
                yield
        else:
            yield

    @pytest.fixture(scope="class")
    def mock_folder_get(test, do_mock, box_error):
        def get(self, *args, **kwargs):
            if self.object_id in test.folders:
                folder = test.folders[self.object_id]
                return boxsdk.object.folder.Folder(None, folder["id"], folder)
            else:
                raise box_error("not_found", _type="folder", object_id=self.object_id)

        if do_mock:
            with pytest.MonkeyPatch.context() as monkeypatch:
                monkeypatch.setattr(boxsdk.object.folder.Folder, "get", get)
                yield
        else:
            yield

    @pytest.fixture(scope="class")
    def mock_file_get(test, do_mock, box_error):
        def get(self, *args, **kwargs):
            if self.object_id in test.file_items:
                return test.file_items[self.object_id]
            else:
                raise box_error("not_found", _type="file", object_id=self.object_id)

        if do_mock:
            with pytest.MonkeyPatch.context() as monkeypatch:
                monkeypatch.setattr(boxsdk.object.file.File, "get", get)
                yield
        else:
            yield
    
    @pytest.fixture(scope="class")
    def mock_item_delete(test, do_mock, box_error):
        def file_delete(self, *args, **kwargs):
            if self.object_id in test.file_items:
                parent = test.file_items[self.object_id].parent["id"]
                found = False
                for i, subitem in enumerate(test.mock_items[parent]):
                    if subitem.id == self.object_id:
                        found = True
                        break
                if found:
                    test.mock_items[parent].pop(i)
                del test.contents[self.object_id]
                del test.file_items[self.object_id]
            else:
                raise box_error("not_found", _type="file", object_id=self.object_id)

        def folder_delete(self, *args, **kwargs):
            if self.object_id in test.folders:
                parent = test.folders[self.object_id]["parent"]["id"]
                found = False
                for i, subitem in enumerate(test.mock_items[parent]):
                    if subitem.id == self.object_id:
                        found = True
                        break
                if found:
                    test.mock_items[parent].pop(i)

                del test.folders[self.object_id]
            else:
                raise box_error("not_found", _type="folder", object_id=self.object_id)

        if do_mock:
            with pytest.MonkeyPatch.context() as monkeypatch:
                monkeypatch.setattr(boxsdk.object.file.File, "delete", file_delete)
                monkeypatch.setattr(
                    boxsdk.object.folder.Folder, "delete", folder_delete
                )
                yield
        else:
            yield


    @pytest.fixture(scope="class")
    def mock_upload(test, do_mock, client, fs, scopes):
        created_files = []

        def upload_stream(self, *args, **kwargs):
            if scopes and TokenScope.ITEM_READWRITE not in scopes:
                raise test.SCOPE_ERROR

            data: IO[bytes] = kwargs["file_stream"]
            file_id = str(test._next_id)
            test._next_id += 1
            data.seek(0, 0)

            file = _build_file(
                self, file_id, data.read(), parent=self.object_id, **kwargs
            )

            test.mock_items[self.object_id].append(file)
            test.file_items[file_id] = file
            return file

        def update_contents_with_stream(self, *args, **kwargs):
            if scopes and TokenScope.ITEM_READWRITE not in scopes:
                raise test.SCOPE_ERROR

            data: IO[bytes] = kwargs["file_stream"]
            file_id = self.object_id
            data.seek(0, 0)
            data_contents = data.read()

            file = _build_file(self, file_id, data_contents, **kwargs)
            
            # Update stored file response object
            test.file_items[file_id]._response_object = file._response_object
            test.file_items[file_id].__dict__.update(file._response_object)

            return file

        def _build_file(self, file_id, data_contents, parent=None, **kwargs):
            test.contents[file_id] = data_contents
            time = datetime.datetime.now().isoformat(timespec="seconds")
            if parent is None:
                parent = self.get(fields=["parent"]).parent["id"]
            path_collection = tuple(test.folders[parent]["path_collection"]["entries"])
            path_collection = path_collection + (test.folders[parent],)

            file = boxsdk.object.file.File(
                session=None,
                object_id=file_id,
                response_object=ItemJSON(
                    name=kwargs["file_name"],
                    id=file_id,
                    created_at=time,
                    modified_at=time,
                    _type="file",
                    path_collection=path_collection,
                    size=len(data_contents)
                ),
            )
            return file

        if do_mock:
            with pytest.MonkeyPatch.context() as monkeypatch:
                monkeypatch.setattr(
                    boxsdk.object.folder.Folder, "upload_stream", upload_stream
                )
                monkeypatch.setattr(
                    boxsdk.object.file.File,
                    "update_contents_with_stream",
                    update_contents_with_stream,
                )
                yield
        else:
            created_files = []
            _original_function = boxsdk.object.folder.Folder.upload_stream

            def wrap(self, *args, **kwargs):
                file = _original_function(self, *args, **kwargs)
                created_files.append(file)
                return file

            with pytest.MonkeyPatch.context() as monkeypatch:
                monkeypatch.setattr(boxsdk.object.folder.Folder, "upload_stream", wrap)
                yield
            for file in created_files:
                try:
                    file.delete()
                except boxsdk.BoxAPIException as e:
                    # Ok if file not found, for remove file test
                    if e.status != 404:
                        raise e

    @pytest.fixture(scope="class")
    def mock_copy(test, fs, do_mock, setup, scopes):
        def copy(self, *, parent_folder, name, file_version=None, **kwargs):
            if scopes and TokenScope.ITEM_READWRITE not in scopes:
                raise test.SCOPE_ERROR

            file_id = None
            for item in test.mock_items[parent_folder.object_id]:
                if item.name == name:
                    file_id = item.object_id
                    break
            if file_id is None:
                file_id = str(test._next_id)
                test._next_id += 1

            time = datetime.datetime.now().isoformat(timespec="seconds")

            file = boxsdk.object.file.File(
                session=None,
                object_id=file_id,
                response_object=ItemJSON(
                    name=name,
                    id=file_id,
                    created_at=time,
                    modified_at=time,
                    _type="file",
                    size=len(test.contents[self.object_id])
                ),
            )
            test.contents[file_id] = test.contents[self.object_id]
            test.mock_items[parent_folder.object_id].append(file)
            test.file_items[file_id] = file

        if do_mock:
            with pytest.MonkeyPatch.context() as monkeypatch:
                monkeypatch.setattr(boxsdk.object.file.File, "copy", copy)
                yield
        else:
            created_files = []
            _original_function = boxsdk.object.file.File.copy

            def wrap(self, *args, **kwargs):
                file = _original_function(self, *args, **kwargs)
                created_files.append(file)
                return file

            with pytest.MonkeyPatch.context() as monkeypatch:
                monkeypatch.setattr(boxsdk.object.file.File, "copy", wrap)
                yield
            for file in created_files:
                file.delete()

    @pytest.fixture(scope="class")
    def mock_file_content(test, do_mock, mock_file_get, box_error):
        def content(self, *args, **kwargs):
            file_id = self.object_id
            if file_id in test.contents:
                return test.contents[file_id]

            raise box_error("not_found", _type="file", object_id=self.object_id)

        if do_mock:
            with pytest.MonkeyPatch.context() as monkeypatch:
                monkeypatch.setattr(boxsdk.object.file.File, "content", content)
                yield
        else:
            yield

    @pytest.fixture(scope="class")
    def mock_create_subfolder(test, fs, do_mock, client, scopes):
        def create_subfolder(self, name):
            if scopes and TokenScope.ITEM_READWRITE not in scopes:
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
            path_collection = copy.deepcopy(
                test.folders[self.object_id]["path_collection"]
            )
            path_collection["total_count"] += 1
            path_collection["entries"] = tuple(path_collection["entries"]) + (
                {
                    "id": self.object_id,
                    "etag": "0",
                    "type": "folder",
                    "sequence_id": None,
                    "name": test.folders[self.object_id]["name"],
                },
            )
            folder_json["path_collection"] = path_collection
            folder = boxsdk.object.folder.Folder(
                session=None, object_id=folder_id, response_object=folder_json
            )
            test.mock_items[self.object_id].append(folder)
            test.folders[folder_id] = folder_json
            return folder

        if do_mock:
            with pytest.MonkeyPatch.context() as monkeypatch:
                monkeypatch.setattr(
                    boxsdk.object.folder.Folder, "create_subfolder", create_subfolder
                )
                yield
        else:
            created_folders = []
            _original_function = boxsdk.object.folder.Folder.create_subfolder

            def wrap(self, *args, **kwargs):
                folder = _original_function(self, *args, **kwargs)
                created_folders.append(folder)
                return folder

            with pytest.MonkeyPatch.context() as monkeypatch:
                monkeypatch.setattr(
                    boxsdk.object.folder.Folder, "create_subfolder", wrap
                )
                yield
            for folder in created_folders:
                try:
                    folder.delete()
                except boxsdk.BoxAPIException:
                    # okay, if parent folder was already deleted
                    pass

    SCOPE_ERROR = boxsdk.BoxAPIException(
        status=403,
        headers=None,
        code="not_found",
        message="Not Found",
        request_id=None,
        url=None,
        method=None,
        context_info={
            "errors": [
                {
                    "reason": "insufficient_scope",
                    "name": "file",
                    "message": "Write permissions not allowed",
                }
            ]
        },
        network_response=None,
    )
