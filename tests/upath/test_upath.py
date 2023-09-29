from contextlib import nullcontext as does_not_raise
import random

import pytest
import upath

from .._utilities import BoxFileSystemMocker
import boxfs  # noqa: F401


@pytest.mark.mock_only
def test_box_protocol_registered():
    assert "box" in upath.registry._registry.known_implementations


@pytest.fixture(
    scope="class",
)
def scopes(request):
    return None


class TestBoxUPath(BoxFileSystemMocker):

    @pytest.fixture(scope="class")
    def test_path(
        self,
        client,
        client_type,
        root_id,
        root_path,
        scopes,
        mock_folder_get,
        mock_create_subfolder,
    ):
        if root_id is None:
            root_id = "0"
        if client is None:
            import fsspec
            client = fsspec.filesystem("box", client_type=client_type).client
        client.folder(root_id).create_subfolder("Test UPath Folder")
        yield upath.UPath(
            "box:///Test UPath Folder",
            client=client,
            root_id=root_id,
            root_path=root_path,
            scopes=scopes
        )
    
    def test_fspath(self, test_path):
        sub_path = test_path / "Subfolder"
        sub_path_url = sub_path.__fspath__()

        assert sub_path_url == "box://Test UPath Folder/Subfolder"

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_create_subfolder",
        "mock_file_get",
        "mock_upload",
        "mock_file_content",
    )
    def test_round_trip(self, test_path):
        # test_path.mkdir()
        file_path = test_path / "round-trip.txt"
        a, b = random.randint(0, 1e9), random.randint(0, 1e9)
        text = f"{a} {b} DONE"

        with file_path.open("wt", encoding="utf-8") as f:
            f.write(text)

        with file_path.open("rt", encoding="utf-8") as f:
            read_text = f.read()

        assert read_text == text

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_file_get",
        "mock_create_subfolder",
    )
    def test_backslashes(self, test_path):
        # test_path.mkdir()
        file_path = test_path / "Subfolder/Inner Folder"
        file_path_backslash = test_path / r"Subfolder\Inner Folder"
        file_path.mkdir(parents=True, exist_ok=True)
        assert file_path.exists()

        with does_not_raise():
            items = set(file_path.iterdir())
            items_backslash = set(file_path_backslash.iterdir())

            assert items == items_backslash

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_file_get",
        "mock_upload",
        "mock_item_delete",
    )
    def test_is_file(self, test_path, do_mock):
        # test_path.mkdir()
        file_path = test_path / "temp-test-file.txt"
        a, b = random.randint(0, 1e9), random.randint(0, 1e9)
        text = f"{a} {b} DONE"

        assert not file_path.is_file()
        with file_path.open("wt", encoding="utf-8") as f:
            f.write(text)
        assert file_path.is_file()
        # if not do_mock:
        #     file_path.unlink()
