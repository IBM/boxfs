"""
Test suite for metadata operations, advanced features, and edge cases
"""

from __future__ import annotations

import datetime
import pytest
import fsspec
from ._utilities import BoxFileSystemMocker


class TestMetadataOperations(BoxFileSystemMocker):
    """Test suite for file metadata operations"""

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
                pass

        yield fs

    # ==================== Timestamp Tests ====================

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_file_get",
    )
    def test_created_timestamp(self, fs):
        """Test getting file creation timestamp"""
        path = "test_file.txt"
        with fs.open(path, "wb") as f:
            f.write(b"test content")

        created = fs.created(path)
        assert isinstance(created, datetime.datetime)

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_file_get",
    )
    def test_modified_timestamp(self, fs):
        """Test getting file modification timestamp"""
        path = "test_file.txt"
        with fs.open(path, "wb") as f:
            f.write(b"test content")

        modified = fs.modified(path)
        assert isinstance(modified, datetime.datetime)

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_file_get",
    )
    def test_timestamps_in_info(self, fs):
        """Test that timestamps are included in file info"""
        path = "test_file.txt"
        with fs.open(path, "wb") as f:
            f.write(b"test content")

        info = fs.info(path)
        assert "created_at" in info
        assert "modified_at" in info
        assert isinstance(info["created_at"], str)
        assert isinstance(info["modified_at"], str)

    # ==================== Signed URL Tests ====================

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_file_get",
    )
    def test_sign_url(self, fs):
        """Test generating signed URL for file"""
        path = "test_file.txt"
        with fs.open(path, "wb") as f:
            f.write(b"test content")

        # sign() should return a URL string
        url = fs.sign(path)
        assert isinstance(url, str)
        # Box download URLs typically contain the file ID
        assert len(url) > 0

    # ==================== File Mode Tests ====================

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_file_content",
    )
    def test_open_read_binary(self, fs):
        """Test opening file in binary read mode"""
        path = "test_file.txt"
        content = b"binary content"

        with fs.open(path, "wb") as f:
            f.write(content)

        with fs.open(path, "rb") as f:
            read_content = f.read()

        assert read_content == content

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_file_content",
    )
    def test_open_read_text(self, fs):
        """Test opening file in text read mode"""
        path = "test_file.txt"
        content = "text content"

        with fs.open(path, "w") as f:
            f.write(content)

        with fs.open(path, "r") as f:
            read_content = f.read()

        assert read_content == content

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
    )
    def test_open_write_binary(self, fs):
        """Test opening file in binary write mode"""
        path = "test_file.txt"
        content = b"binary write"

        with fs.open(path, "wb") as f:
            f.write(content)

        assert fs.exists(path)

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
    )
    def test_open_write_text(self, fs):
        """Test opening file in text write mode"""
        path = "test_file.txt"
        content = "text write"

        with fs.open(path, "w") as f:
            f.write(content)

        assert fs.exists(path)

    # ==================== Path Manipulation Tests ====================

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
    )
    def test_strip_protocol(self, fs):
        """Test protocol stripping from paths"""
        # Test various path formats
        assert fs._strip_protocol("box://path/to/file.txt") == "/path/to/file.txt"
        assert fs._strip_protocol("/path/to/file.txt") == "/path/to/file.txt"
        assert fs._strip_protocol("path/to/file.txt") == "/path/to/file.txt"

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
    )
    def test_strip_protocol_backslashes(self, fs):
        """Test protocol stripping handles backslashes"""
        # Windows-style paths should be converted to forward slashes
        result = fs._strip_protocol(r"path\to\file.txt")
        assert "\\" not in result
        assert result == "/path/to/file.txt"

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
    )
    def test_parent_path(self, fs):
        """Test getting parent directory path"""
        assert fs._parent("/path/to/file.txt") == "/path/to"
        assert fs._parent("/path/to/") == "/path"
        assert fs._parent("/file.txt") == "/"

    # ==================== Cache and Options Tests ====================

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
    )
    def test_option_context(self, fs):
        """Test option_context for temporary option changes"""
        # Default refresh option
        assert fs.default_options["refresh"] is False

        # Change option temporarily
        with fs.option_context(refresh=True):
            assert fs.default_options["refresh"] is True

        # Should revert after context
        assert fs.default_options["refresh"] is False

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_file_get",
    )
    def test_ls_refresh_option(self, fs):
        """Test ls with refresh parameter"""
        path = "test_file.txt"
        with fs.open(path, "wb") as f:
            f.write(b"content")

        # List with refresh=False (use cache)
        items1 = fs.ls("/", refresh=False)

        # List with refresh=True (bypass cache)
        items2 = fs.ls("/", refresh=True)

        # Both should return items
        assert len(items1) > 0
        assert len(items2) > 0

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
    )
    def test_invalidate_cache(self, fs):
        """Test cache invalidation"""
        path = "test_file.txt"
        with fs.open(path, "wb") as f:
            f.write(b"content")

        # Populate cache
        fs.ls("/", refresh=True)

        # Invalidate cache
        fs.invalidate_cache()

        # Cache should be cleared
        assert len(fs.dircache) == 0

    # ==================== Edge Cases ====================

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
    )
    def test_empty_file_operations(self, fs):
        """Test operations on empty files"""
        path = "empty.txt"

        # Create empty file
        with fs.open(path, "wb"):
            pass

        # Should exist and have size 0
        assert fs.exists(path)
        assert fs.size(path) == 0

        # Should be able to read empty content
        content = fs.cat(path)
        assert content == b""

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
    )
    def test_special_characters_in_filename(self, fs):
        """Test files with special characters in names"""
        # Test various special characters
        special_names = [
            "file with spaces.txt",
            "file-with-dashes.txt",
            "file_with_underscores.txt",
            "file.multiple.dots.txt",
        ]

        for name in special_names:
            with fs.open(name, "wb") as f:
                f.write(b"content")

            assert fs.exists(name)
            assert fs.isfile(name)

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_create_subfolder",
    )
    def test_deeply_nested_paths(self, fs):
        """Test operations on deeply nested directory structures"""
        deep_path = "/".join([f"level{i}" for i in range(5)])

        fs.mkdir(deep_path, create_parents=True)

        assert fs.exists(deep_path)
        assert fs.isdir(deep_path)

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_file_content",
    )
    def test_file_overwrite(self, fs):
        """Test overwriting existing file"""
        path = "overwrite.txt"

        # Write initial content
        with fs.open(path, "wb") as f:
            f.write(b"original")

        # Overwrite with new content
        with fs.open(path, "wb") as f:
            f.write(b"updated")

        # Should have new content
        content = fs.cat(path)
        assert content == b"updated"

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
    )
    def test_root_directory_operations(self, fs):
        """Test operations on root directory"""
        # Root should exist
        assert fs.exists("/")

        # Root should be a directory
        assert fs.isdir("/")

        # Should be able to list root
        items = fs.ls("/")
        assert isinstance(items, list)

    # ==================== Error Handling Tests ====================

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
    )
    def test_file_not_found_errors(self, fs):
        """Test that appropriate errors are raised for missing files"""
        nonexistent = "nonexistent.txt"

        with pytest.raises(FileNotFoundError):
            fs.info(nonexistent)

        with pytest.raises(FileNotFoundError):
            fs.size(nonexistent)

        with pytest.raises(FileNotFoundError):
            fs.cat(nonexistent)

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
    )
    def test_file_exists_errors(self, fs):
        """Test that appropriate errors are raised for existing files"""
        path = "existing.txt"

        with fs.open(path, "wb") as f:
            f.write(b"content")

        # mkdir should fail if file exists with same name
        with pytest.raises((FileExistsError, Exception)):
            fs.mkdir(path)

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_create_subfolder",
    )
    def test_directory_exists_errors(self, fs):
        """Test errors when directory already exists"""
        path = "existing_dir"

        fs.mkdir(path)

        # mkdir without exist_ok should fail
        with pytest.raises(FileExistsError):
            fs.mkdir(path, create_parents=False)

    # ==================== Batch Operations Tests ====================

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_file_content",
    )
    def test_cat_ranges(self, fs):
        """Test reading file ranges"""
        path = "range_test.txt"
        content = b"0123456789" * 10  # 100 bytes

        with fs.open(path, "wb") as f:
            f.write(content)

        # Read specific range
        start, end = 10, 20
        range_content = fs.cat_file(path, start=start, end=end)

        assert len(range_content) == end - start
        assert range_content == content[start:end]

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_item_delete",
    )
    def test_rm_multiple_files(self, fs):
        """Test removing multiple files at once"""
        files = ["file1.txt", "file2.txt", "file3.txt"]

        # Create files
        for file_path in files:
            with fs.open(file_path, "wb") as file:
                file.write(b"content")

        # Remove all files
        fs.rm(files)

        # All should be deleted
        for f in files:
            assert not fs.exists(f)

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_file_content",
    )
    def test_get_put_operations(self, fs, tmp_path):
        """Test get/put operations with local filesystem"""
        import os

        remote_path = "remote_file.txt"
        local_path = os.path.join(tmp_path, "local_file.txt")
        content = b"test content for get/put"

        # Create remote file
        with fs.open(remote_path, "wb") as f:
            f.write(content)

        # Get file to local
        fs.get(remote_path, local_path)

        # Verify local file exists and has correct content
        assert os.path.exists(local_path)
        with open(local_path, "rb") as f:
            assert f.read() == content

        # Modify local file
        new_content = b"modified content"
        with open(local_path, "wb") as f:
            f.write(new_content)

        # Put file back to remote
        remote_path2 = "remote_file2.txt"
        fs.put(local_path, remote_path2)

        # Verify remote file has new content
        assert fs.cat(remote_path2) == new_content

# Made with Bob
