from __future__ import annotations

import random
from contextlib import nullcontext as does_not_raise

import boxsdk
import boxsdk.object.file
import boxsdk.object.folder
import boxsdk.object.search
import fsspec
import pytest
from boxsdk.auth.oauth2 import TokenScope
from ._utilities import BoxFileSystemMocker


@pytest.mark.mock_only
def test_box_protocol_registered():
    assert "box" in fsspec.available_protocols()


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


class TestBoxFileSystem(BoxFileSystemMocker):
    @pytest.fixture(scope="class")
    def fs(self, client, root_id, root_path, mock_folder_get, scopes):
        fs = fsspec.filesystem("box", client=client.clone(), root_id=root_id)

        if scopes:
            try:
                fs.downscope_token(scopes=scopes)
            except AttributeError:
                # Fails during mock because there's auth is None
                pass

        yield fs

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
            for i in range(2):
                # Use different ranges for each call to test that the file size updates
                # correctly
                _min, _max = i * 1e5, 10**(5 * (i+1)) - 1
                a, b = random.randint(_min, _max), random.randint(_min, _max)
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
