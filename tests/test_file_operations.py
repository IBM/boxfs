"""
Test suite for file operations: move, rename, size, metadata, and I/O operations
"""

from __future__ import annotations

import random

import box_sdk_gen

import fsspec
import pytest
from ._utilities import BoxFileSystemMocker


@pytest.fixture(
    scope="class",
    params=[
        pytest.param(None, id="no-scope"),
    ],
)
def scopes(request):
    return request.param


class TestFileOperations(BoxFileSystemMocker):
    """Test suite for file operations including move, rename, size, and metadata"""

    @pytest.fixture(scope="function")
    def fs(self, client, client_type, root_id, root_path, mock_folder_get, scopes):  # noqa: F811
        fs = fsspec.filesystem(
            "box",
            client=client,
            root_id=root_id,
            client_type=client_type,
            cache_paths=False,
            skip_instance_cache=True,
        )

        if scopes:
            try:
                fs.downscope_token(scopes=scopes)
            except AttributeError:
                # Fails during mock because auth is None
                pass

        yield fs

    @pytest.fixture(scope="function")
    def write_file(self, fs, mock_upload, mock_folder_get, mock_folder_get_items):
        """Helper fixture to write a file with random content"""

        def _write(path, size_range=(0, 1000)):
            a, b = random.randint(*size_range), random.randint(*size_range)
            text = f"{a} {b} DONE"
            with fs.open(path, "wb") as f:
                f.write(text.encode())
            return text

        yield _write

    # ==================== File Move/Rename Tests ====================

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_copy",
        "mock_item_delete",
        "mock_file_content",
    )
    def test_move_file_same_directory(self, fs, write_file):
        """Test moving/renaming file within same directory"""
        src_path = "source_file.txt"
        dest_path = "renamed_file.txt"

        text = write_file(src_path)
        assert fs.exists(src_path)

        # Move (rename) the file
        fs.mv(src_path, dest_path)

        # Source should not exist, destination should exist with same content
        assert not fs.exists(src_path)
        assert fs.exists(dest_path)
        assert fs.cat(dest_path).decode() == text

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_copy",
        "mock_item_delete",
        "mock_file_content",
        "mock_create_subfolder",
    )
    def test_move_file_different_directory(self, fs, write_file):
        """Test moving file to different directory"""
        src_path = "source_file.txt"
        dest_folder = "destination_folder"
        dest_path = f"{dest_folder}/moved_file.txt"

        text = write_file(src_path)
        fs.mkdir(dest_folder)

        # Move the file
        fs.mv(src_path, dest_path)

        # Source should not exist, destination should exist
        assert not fs.exists(src_path)
        assert len(fs.ls("", detail=False)) == 1
        assert fs.ls("", detail=False)[0] == "/destination_folder"
        assert fs.exists(dest_path)
        assert fs.cat(dest_path).decode() == text

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_copy",
        "mock_item_delete",
        "mock_file_content",
    )
    def test_move_file_overwrite_fails(self, fs, write_file):
        """Test that moving to existing file fails by default"""
        src_path = "source_file.txt"
        dest_path = "dest_file.txt"

        write_file(src_path)
        write_file(dest_path)

        # Move should fail when destination exists
        with pytest.raises(FileExistsError):
            fs.mv(src_path, dest_path)

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_copy",
        "mock_item_delete",
    )
    def test_move_nonexistent_file(self, fs):
        """Test moving non-existent file raises error"""
        src_path = "nonexistent.txt"
        dest_path = "destination.txt"

        with pytest.raises(FileNotFoundError):
            fs.mv(src_path, dest_path)

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_copy",
        "mock_item_delete",
        "mock_file_content",
        "mock_create_subfolder",
    )
    def test_rename_file(self, fs, write_file):
        """Test renaming file using rename method"""
        old_path = "old_name.txt"
        new_path = "new_name.txt"

        text = write_file(old_path)

        # Rename the file (fsspec's rename is an alias for mv)
        fs.rename(old_path, new_path)

        assert not fs.exists(old_path)
        assert fs.exists(new_path)
        assert fs.cat(new_path).decode() == text

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_copy",
        "mock_item_delete",
        "mock_file_content",
        "mock_create_subfolder",
    )
    def test_move_folder(self, fs, write_file):
        """Test moving entire folder"""
        src_folder = "source_folder"
        dest_folder = "dest_folder"
        file_in_folder = f"{src_folder}/file.txt"

        fs.mkdir(src_folder)
        text = write_file(file_in_folder)

        # Move the folder
        fs.mv(src_folder, dest_folder, recursive=True)

        # Source folder should not exist
        assert not fs.exists(src_folder)
        # Destination folder should exist with file
        assert fs.exists(dest_folder)
        assert fs.exists(f"{dest_folder}/file.txt")
        assert fs.cat(f"{dest_folder}/file.txt").decode() == text

    # ==================== File Size Tests ====================

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_file_get",
    )
    def test_file_size_empty(self, fs):
        """Test size of empty file"""
        path = "empty_file.txt"

        with fs.open(path, "wb") as f:
            f.write(b"")

        size = fs.size(path)
        assert size == 0

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_file_get",
    )
    def test_file_size_small(self, fs, write_file):
        """Test size of small file"""
        path = "small_file.txt"
        text = write_file(path, size_range=(10, 100))

        size = fs.size(path)
        assert size == len(text.encode())

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_file_get",
    )
    def test_file_size_via_info(self, fs, write_file):
        """Test getting file size via info method"""
        path = "test_file.txt"
        text = write_file(path)

        info = fs.info(path)
        assert info["size"] == len(text.encode())
        assert info["type"] == "file"

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
    )
    def test_size_nonexistent_file(self, fs):
        """Test size of non-existent file raises error"""
        with pytest.raises(FileNotFoundError):
            fs.size("nonexistent.txt")

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_file_get",
    )
    def test_file_sizes_multiple(self, fs, write_file):
        """Test getting sizes of multiple files"""
        files = ["file1.txt", "file2.txt", "file3.txt"]
        texts = [write_file(f) for f in files]

        sizes = fs.sizes(files)
        for i, size in enumerate(sizes):
            assert size == len(texts[i].encode())

    # ==================== File Metadata Tests ====================

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_file_get",
    )
    def test_file_created_time(self, fs, write_file):
        """Test getting file creation time"""
        import datetime

        path = "test_file.txt"
        write_file(path)

        created = fs.created(path)
        assert isinstance(created, datetime.datetime)

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_file_get",
    )
    def test_file_modified_time(self, fs, write_file):
        """Test getting file modification time"""
        import datetime

        path = "test_file.txt"
        write_file(path)

        modified = fs.modified(path)
        assert isinstance(modified, datetime.datetime)

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_file_get",
    )
    def test_file_info_complete(self, fs, write_file):
        """Test that file info contains all expected fields"""
        path = "test_file.txt"
        write_file(path)

        info = fs.info(path)

        # Check required fields
        assert "name" in info
        assert "size" in info
        assert "type" in info
        assert "created_at" in info
        assert "modified_at" in info
        assert "id" in info
        assert "etag" in info

        assert info["type"] == "file"
        assert info["name"].endswith(path)

    # ==================== File I/O Tests ====================

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_file_content",
    )
    def test_cat_file(self, fs, write_file):
        """Test reading entire file with cat_file"""
        path = "test_file.txt"
        text = write_file(path)

        content = fs.cat_file(path)
        assert content.decode() == text

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_file_content",
    )
    def test_cat_multiple_files(self, fs, write_file):
        """Test reading multiple files with cat"""
        files = ["file1.txt", "file2.txt", "file3.txt"]
        texts = [write_file(f) for f in files]

        contents = fs.cat(files)
        assert isinstance(contents, dict)
        for i, path in enumerate(files):
            assert contents[path].decode() == texts[i]

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_file_content",
    )
    def test_pipe_file(self, fs):
        """Test piping data directly to file"""
        path = "piped_file.txt"
        data = b"This is piped data"

        fs.pipe_file(path, data)

        assert fs.exists(path)
        assert fs.cat(path) == data

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_file_content",
    )
    def test_pipe_multiple_files(self, fs):
        """Test piping data to multiple files"""
        files_data = {
            "file1.txt": b"Data 1",
            "file2.txt": b"Data 2",
            "file3.txt": b"Data 3",
        }

        fs.pipe(files_data)

        for path, data in files_data.items():
            assert fs.exists(path)
            assert fs.cat(path) == data

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_file_content",
    )
    def test_head_file(self, fs, write_file):
        """Test reading first N bytes of file"""
        path = "test_file.txt"
        text = "A" * 100  # 100 bytes
        with fs.open(path, "wb") as f:
            f.write(text.encode())

        # Read first 10 bytes
        head = fs.head(path, size=10)
        assert head == b"A" * 10

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_file_content",
    )
    def test_tail_file(self, fs, write_file):
        """Test reading last N bytes of file"""
        path = "test_file.txt"
        text = "A" * 90 + "B" * 10  # 100 bytes total, last 10 are 'B'
        with fs.open(path, "wb") as f:
            f.write(text.encode())

        # Read last 10 bytes
        tail = fs.tail(path, size=10)
        assert tail == b"B" * 10

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_file_content",
    )
    def test_touch_creates_file(self, fs):
        """Test touch creates empty file if it doesn't exist"""
        path = "touched_file.txt"

        assert not fs.exists(path)
        fs.touch(path)
        assert fs.exists(path)
        assert fs.size(path) == 0

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_file_content",
        "mock_file_get",
    )
    def test_touch_preserves_content(self, fs, write_file):
        """Test touch doesn't truncate existing file by default"""
        path = "existing_file.txt"
        text = write_file(path)

        fs.touch(path)

        # Content should be preserved (truncate=False by default)
        assert fs.cat(path).decode() == text

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_file_content",
        "mock_file_get",
    )
    def test_touch_truncate(self, fs, write_file):
        """Test touch with truncate=True empties file"""
        path = "existing_file.txt"
        write_file(path)

        fs.touch(path, truncate=True)

        # File should be empty after truncate
        assert fs.size(path) == 0

    # ==================== File Existence Tests ====================

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
    )
    def test_exists_file(self, fs, write_file):
        """Test exists returns True for existing file"""
        path = "test_file.txt"
        write_file(path)

        assert fs.exists(path)

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
    )
    def test_exists_nonexistent(self, fs):
        """Test exists returns False for non-existent file"""
        assert not fs.exists("nonexistent.txt")

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_create_subfolder",
    )
    def test_exists_folder(self, fs):
        """Test exists returns True for existing folder"""
        path = "test_folder"
        fs.mkdir(path)

        assert fs.exists(path)

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
    )
    def test_isfile(self, fs, write_file):
        """Test isfile returns True for files"""
        path = "test_file.txt"
        write_file(path)

        assert fs.isfile(path)

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_create_subfolder",
    )
    def test_isfile_folder(self, fs):
        """Test isfile returns False for folders"""
        path = "test_folder"
        fs.mkdir(path)

        assert not fs.isfile(path)

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
    )
    def test_isdir_file(self, fs, write_file):
        """Test isdir returns False for files"""
        path = "test_file.txt"
        write_file(path)

        assert not fs.isdir(path)

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_create_subfolder",
    )
    def test_isdir_folder(self, fs):
        """Test isdir returns True for folders"""
        path = "test_folder"
        fs.mkdir(path)

        assert fs.isdir(path)

    # ==================== File Checksum Tests ====================

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_file_get",
    )
    def test_checksum_consistency(self, fs, write_file):
        """Test that checksum is consistent across reads"""
        path = "test_file.txt"
        write_file(path)

        # Get file info which includes etag (Box's version of checksum)
        info1 = fs.info(path)
        info2 = fs.info(path)

        assert "etag" in info1
        assert info1["etag"] == info2["etag"]

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_file_get",
        "mock_file_content",
    )
    def test_checksum_changes_after_modification(self, fs, write_file):
        """Test that checksum changes after file modification"""
        path = "test_file.txt"
        write_file(path)

        info1 = fs.info(path)
        etag1 = info1["etag"]

        # Modify the file
        with fs.open(path, "wb") as f:
            f.write(b"Modified content")

        info2 = fs.info(path)
        etag2 = info2["etag"]

        # ETags should be different after modification
        assert etag1 != etag2

# Made with Bob
