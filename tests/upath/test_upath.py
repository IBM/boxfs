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

    @pytest.fixture(scope="function")
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
