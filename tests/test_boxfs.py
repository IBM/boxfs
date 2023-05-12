from __future__ import annotations

import copy
import datetime
import random
from collections import defaultdict
from contextlib import nullcontext as does_not_raise
from typing import IO

import boxsdk
import boxsdk.object.file
import boxsdk.object.folder
import boxsdk.object.search
import fsspec
import pytest
from boxsdk.auth.oauth2 import TokenScope
from boxsdk.session.session import Session
from _utilities import ItemJSON, MockedCollection


@pytest.mark.mock_only
def test_box_protocol_registered():
    assert "box" in fsspec.available_protocols()


USER_ROOT = {
    "id": "0",
    "etag": "0",
    "type": "folder",
    "sequence_id": None,
    "name": "All Files",
}


@pytest.fixture(
    scope="class",
    params=[
        pytest.param(None, id="no-scope"),
        pytest.param((TokenScope.ITEM_READWRITE,), id="read-write"),
        pytest.param((TokenScope.ITEM_READ,), id="read"),
    ],
)
def scopes(request):
    return request.param


@pytest.fixture(scope="class")
def write_expectation(scopes, request):
    """Context manager to specify whether test should succeed/fail based on scope"""
    if scopes is None or TokenScope.ITEM_READWRITE in scopes:
        yield does_not_raise()
    else:
        yield pytest.raises(boxsdk.BoxAPIException, match="403")

BOX_CODES = {
    "not_found": {
        "status": 404,
        "message": "Not Found",
        "reason": "invalid_parameter",
        "error_message": (
            "Invalid value '{object_id}'. '{_type}' with value '{object_id}' not found"
        ),
    }
}

@pytest.fixture(scope="session")
def box_error():
    def _error(code, _type=None, **kwargs):
        error_details = BOX_CODES[code]
        return boxsdk.BoxAPIException(
            status=error_details["status"],
            headers=None,
            code=code,
            message=error_details["message"],
            request_id=None,
            url=None,
            method=None,
            context_info={
                "errors": [
                    {
                        "reason": error_details["reason"],
                        "name": kwargs.get("_type", ""),
                        "message": error_details["error_message"].format(**kwargs),
                    }
                ]
            },
            network_response=None,
        )
    yield _error

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
                self.mock_items[_folder_id].append(folder)
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
    def mock_upload(test, do_mock, client, fs):
        created_files = []

        def upload_stream(self, *args, **kwargs):
            if fs.scopes and TokenScope.ITEM_READWRITE not in fs.scopes:
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
            if fs.scopes and TokenScope.ITEM_READWRITE not in fs.scopes:
                raise test.SCOPE_ERROR

            data: IO[bytes] = kwargs["file_stream"]
            file_id = self.object_id
            data.seek(0, 0)

            file = _build_file(self, file_id, data.read(), **kwargs)

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
    def mock_copy(test, fs, do_mock, setup):
        def copy(self, *, parent_folder, name, file_version=None, **kwargs):
            if fs.scopes and TokenScope.ITEM_READWRITE not in fs.scopes:
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
    def mock_create_subfolder(test, fs, do_mock, client):
        def create_subfolder(self, name):
            if fs.scopes and TokenScope.ITEM_READWRITE not in fs.scopes:
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


class TestBoxFileSystem(BoxFileSystemMocker):
    @pytest.fixture(scope="class")
    def fs(self, client, root_id, root_path, mock_folder_get, scopes):
        fs = fsspec.filesystem("box", client=client.clone(), root_id=root_id)

        if scopes:
            fs.scopes = scopes
            try:
                fs.downscope_token(scopes=fs.scopes)
            except AttributeError:
                # Fails during mock because there's auth is None
                pass

        yield fs
        fs.scopes = None

    @pytest.fixture(scope="class")
    def write_file(self, fs, mock_upload, mock_folder_get, mock_folder_get_items):
        def _write(path):
            a, b = random.randint(0, 1e9), random.randint(0, 1e9)
            text = f"{a} {b} DONE"
            with fs.open(path, "wb") as f:
                f.write(text.encode())
            return text
        yield _write


    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
    )
    def test_box_protocol(self, client, root_id, root_path):  # noqa: F811
        """Filesystem instantiates correctly with root_id or root_path"""
        fs = fsspec.filesystem("box", client=client, root_id=root_id)
        assert fs.root_id and fs.root_path

        fs = fsspec.filesystem("box", client=client, root_path=root_path)
        assert fs.root_id and fs.root_path

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_file_get",
        "mock_upload",
        "mock_file_content",
    )
    def test_box_round_trip(self, fs, write_expectation):
        """File writes and reads correctly"""
        path = "round_trip.txt"

        with write_expectation:
            # Go twice to test upload + update
            for _ in range(2):
                a, b = random.randint(0, 1e9), random.randint(0, 1e9)
                text = f"{a} {b} DONE"
                with fs.open(path, "wb") as f:
                    f.write(text.encode())
                file_contents: bytes = fs.cat(path)
                assert file_contents.decode() == text
    
    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_item_delete",
    )
    def test_box_remove(self, fs, write_expectation, write_file):
        """File is deleted correctly"""
        path = "removeable_file.txt"

        with write_expectation:
            write_file(path)
            fs.rm(path)

            assert not fs.exists(path)


    @pytest.mark.usefixtures(
        "mock_file_content",
        "mock_copy",
    )
    def test_box_copy_file(self, fs, write_expectation, write_file):
        """File copies successfully, including failure on existing file"""
        src_path = "source_file.txt"
        dest_path = "dest_file.txt"

        with write_expectation:
            # Go twice to test copy + override
            text = write_file(src_path)

            fs.copy(src_path, dest_path)
            file_contents: bytes = fs.cat(dest_path)
            assert file_contents.decode() == text

            write_file(src_path)
            with pytest.raises(FileExistsError):
                fs.copy(src_path, dest_path)

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_create_subfolder",
    )
    def test_box_mkdir(self, fs, write_expectation):
        """Create folders and nested folders correctly"""
        path = "Subfolder"

        with write_expectation:
            fs.mkdir(path)
            items = fs.ls("/", refresh=True)
            assert any(item["name"] == path for item in items)

            path2 = "Subfolder 2/Subsubfolder"
            with pytest.raises(FileNotFoundError):
                fs.mkdir(path2, create_parents=False)
            items = fs.ls("/", refresh=True)
            assert not any(item["name"] == path2 for item in items)

            fs.mkdir(path2, create_parents=True)
            items = fs.ls("/", refresh=True)
            assert any(item["name"] == "Subfolder 2" for item in items)
            items2 = fs.ls("Subfolder 2", refresh=True)
            assert any(item["name"] == "Subfolder 2/Subsubfolder" for item in items2)
