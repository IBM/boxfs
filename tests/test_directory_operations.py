"""
Test suite for directory operations: walk, find, glob, directory traversal, rmdir, mkdir
"""

from __future__ import annotations

import pytest
import fsspec
from ._utilities import BoxFileSystemMocker


@pytest.fixture(
    scope="class",
    params=[
        pytest.param(None, id="no-scope"),
    ],
)
def scopes(request):
    return request.param



class TestDirectoryOperations(BoxFileSystemMocker):
    """Test suite for directory operations including walk, find, and glob"""

    @pytest.fixture(scope="function")
    def fs(self, client, client_type, root_id, root_path, mock_folder_get):  # noqa: F811
        fs = fsspec.filesystem(
            "box",
            client=client,
            root_id=root_id,
            client_type=client_type,
            cache_paths=False,
            skip_instance_cache=True,
        )

        # if scopes:
        #     try:
        #         fs.downscope_token(scopes=scopes)
        #     except AttributeError:
        #         # Fails during mock because auth is None
        #         pass

        yield fs

    @pytest.fixture(scope="function")
    def create_test_tree(self, fs, mock_upload, mock_create_subfolder):
        """Create a test directory tree structure"""

        def _create():
            # Create directory structure:
            # /
            # ├── folder1/
            # │   ├── file1.txt
            # │   └── subfolder1/
            # │       └── file2.txt
            # ├── folder2/
            # │   └── file3.txt
            # └── file4.txt

            fs.mkdir("folder1")
            fs.mkdir("folder1/subfolder1")
            fs.mkdir("folder2")

            with fs.open("folder1/file1.txt", "wb") as f:
                f.write(b"content1")
            with fs.open("folder1/subfolder1/file2.txt", "wb") as f:
                f.write(b"content2")
            with fs.open("folder2/file3.txt", "wb") as f:
                f.write(b"content3")
            with fs.open("file4.txt", "wb") as f:
                f.write(b"content4")

        yield _create

    # ==================== Walk Tests ====================

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_create_subfolder",
    )
    def test_walk_simple(self, fs, create_test_tree):
        """Test walking through directory tree"""
        create_test_tree()

        walked_paths = []
        for root, dirs, files in fs.walk("/"):
            walked_paths.append((root, sorted(dirs), sorted(files)))

        # Should visit root and all subdirectories
        assert len(walked_paths) > 0
        # Root should have folders and files
        root_entry = [p for p in walked_paths if p[0] == "/"][0]
        # no leading slash - matches MemoryFileSystem
        assert "folder1" in root_entry[1]
        assert "file4.txt" in root_entry[2]
        
        folder1 = [p for p in walked_paths if p[0] == "/folder1"][0]
        assert "subfolder1" in folder1[1]
        assert "file1.txt" in folder1[2]

        subfolder1 = [p for p in walked_paths if p[0] == "/folder1/subfolder1"][0]
        assert "file2.txt" in subfolder1[2]
        assert len(subfolder1[1]) == 0


    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_create_subfolder",
    )
    def test_walk_maxdepth(self, fs, create_test_tree):
        """Test walking with maximum depth limit"""
        create_test_tree()

        # Walk with maxdepth=1 (only root level)
        walked_paths = list(fs.walk("/", maxdepth=1))

        # Should only visit root directory
        assert len(walked_paths) == 1
        assert walked_paths[0][0] == "/"

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_create_subfolder",
    )
    def test_walk_topdown(self, fs, create_test_tree):
        """Test walking top-down vs bottom-up"""
        create_test_tree()

        # Top-down walk
        topdown_paths = [root for root, _, _ in fs.walk("/", topdown=True)]

        # Bottom-up walk
        bottomup_paths = [root for root, _, _ in fs.walk("/", topdown=False)]

        # Both should visit same paths, but in different order
        assert set(topdown_paths) == set(bottomup_paths)
        # Root should be first in topdown
        assert topdown_paths[0] == "/"
        # Root should be last in bottomup
        assert bottomup_paths[-1] == "/"

    # ==================== Find Tests ====================

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_create_subfolder",
    )
    def test_find_all_files(self, fs, create_test_tree):
        """Test finding all files in directory tree"""
        create_test_tree()

        files = fs.find("/")

        # Should find all 4 files
        assert len(files) >= 4
        # Check that specific files are found
        file_names = [f.split("/")[-1] for f in files]
        assert "file1.txt" in file_names
        assert "file2.txt" in file_names
        assert "file3.txt" in file_names
        assert "file4.txt" in file_names

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_create_subfolder",
    )
    def test_find_with_maxdepth(self, fs, create_test_tree):
        """Test finding files with depth limit"""
        create_test_tree()

        # Find only at root level
        files = fs.find("/", maxdepth=1)

        # Should only find file4.txt at root
        file_names = [f.split("/")[-1] for f in files]
        assert "file4.txt" in file_names
        # Should not find nested files
        assert "file2.txt" not in file_names

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_create_subfolder",
    )
    def test_find_with_detail(self, fs, create_test_tree):
        """Test finding files with detailed information"""
        create_test_tree()

        files = fs.find("/", detail=True)

        # Should return dict with file info
        assert isinstance(files, dict)
        # Each entry should have file metadata
        for path, info in files.items():
            assert "size" in info
            assert "type" in info
            assert "id" in info
            assert "etag" in info
            assert "modified_at" in info
            assert "created_at" in info

            assert info["type"] == "file"
            assert info["size"] == 8

    # ==================== Glob Tests ====================

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_create_subfolder",
    )
    def test_glob_all_txt_files(self, fs, create_test_tree):
        """Test globbing for all .txt files"""
        create_test_tree()

        files = fs.glob("**/*.txt")

        # Should find all 4 .txt files
        assert len(files) >= 4
        # All should end with .txt
        assert all(f.endswith(".txt") for f in files)

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_create_subfolder",
    )
    def test_glob_specific_folder(self, fs, create_test_tree):
        """Test globbing within specific folder"""
        create_test_tree()

        files = fs.glob("folder1/*.txt")

        # Should find file1.txt in folder1
        file_names = [f.split("/")[-1] for f in files]
        assert "file1.txt" in file_names
        # Should not find file2.txt (in subfolder)
        assert "file2.txt" not in file_names

        # ensure full path is returned
        file1 = [f for f in files][0]
        assert "/folder1/file1.txt" in file1

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_create_subfolder",
    )
    def test_glob_recursive(self, fs, create_test_tree):
        """Test recursive globbing"""
        create_test_tree()

        files = fs.glob("folder1/**/*.txt")

        # Should find both file1.txt and file2.txt
        file_names = [f.split("/")[-1] for f in files]
        assert "file1.txt" in file_names
        assert "file2.txt" in file_names

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_create_subfolder",
    )
    def test_glob_with_detail(self, fs, create_test_tree):
        """Test globbing with detailed information"""
        create_test_tree()

        files = fs.glob("**/*.txt", detail=True)

        # Should return dict with file info
        assert isinstance(files, dict)
        for path, info in files.items():
            assert "size" in info
            assert "type" in info

    # ==================== Directory Listing Tests ====================

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_create_subfolder",
    )
    def test_ls(self, fs, create_test_tree):
        """Test directory listing"""
        create_test_tree()

        # Non-recursive ls
        items = fs.ls("/", detail=False)
        assert len(items) == 3  # folder1, folder2, file4.txt

        # Check that we can list subdirectories
        folder1_items = fs.ls("folder1", detail=False)
        assert len(folder1_items) == 2  # file1.txt, subfolder1

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_create_subfolder",
    )
    def test_ls_empty_directory(self, fs):
        """Test listing empty directory"""
        fs.mkdir("empty_folder")

        items = fs.ls("empty_folder", detail=False)
        assert len(items) == 0

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
    )
    def test_ls_nonexistent_directory(self, fs):
        """Test listing non-existent directory raises error"""
        with pytest.raises(FileNotFoundError):
            fs.ls("nonexistent_folder")

    # ==================== Directory Size Tests ====================

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_create_subfolder",
    )
    def test_du_directory(self, fs, create_test_tree):
        """Test calculating directory size"""
        create_test_tree()

        # Get size of folder1 (should include subfolder)
        size = fs.du("folder1", total=True)

        # Should be sum of file1.txt and file2.txt
        assert size == 16

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_create_subfolder",
    )
    def test_du_with_maxdepth(self, fs, create_test_tree):
        """Test calculating directory size with depth limit"""
        create_test_tree()

        # Get size with maxdepth=1 (only direct children)
        size_shallow = fs.du("folder1", total=True, maxdepth=1)

        # Get size with no depth limit
        size_deep = fs.du("folder1", total=True)

        # Deep should be >= shallow (includes subfolder)
        assert size_deep > size_shallow

    # ==================== Directory Info Tests ====================

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_create_subfolder",
    )
    def test_info_directory(self, fs):
        """Test getting directory info"""
        fs.mkdir("test_folder")

        info = fs.info("test_folder")

        assert info["type"] == "directory"
        assert "name" in info
        # full path
        assert info["name"] == "/test_folder"
        assert "id" in info
        assert "modified_at" in info
        assert "created_at" in info
        assert "etag" in info

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_create_subfolder",
    )
    def test_isdir_and_isfile(self, fs, mock_upload):
        """Test isdir and isfile methods"""
        fs.mkdir("test_folder")

        with fs.open("test_file.txt", "wb") as f:
            f.write(b"test")

        assert fs.isdir("test_folder")
        assert not fs.isfile("test_folder")

        assert fs.isfile("test_file.txt")
        assert not fs.isdir("test_file.txt")

    # ==================== Directory Removal Tests ====================

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_create_subfolder",
        "mock_item_delete",
    )
    def test_rmdir_empty(self, fs):
        """Test removing empty directory"""
        fs.mkdir("empty_folder")
        assert fs.exists("empty_folder")

        fs.rmdir("empty_folder")
        assert not fs.exists("empty_folder")

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_upload",
        "mock_create_subfolder",
        "mock_item_delete",
    )
    def test_rmdir_recursive(self, fs, create_test_tree):
        """Test removing directory recursively"""
        create_test_tree()

        # Remove folder1 and all its contents
        fs.rmdir("folder1", recursive=True)

        assert not fs.exists("folder1")
        assert not fs.exists("folder1/file1.txt")
        assert not fs.exists("folder1/subfolder1")

    # ==================== Directory Creation Edge Cases ====================

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_create_subfolder",
    )
    def test_makedirs_exist_ok(self, fs):
        """Test makedirs with exist_ok parameter"""
        fs.mkdir("test_folder")

        # Should not raise error with exist_ok=True
        fs.makedirs("test_folder", exist_ok=True)

        # Should raise error with exist_ok=False
        with pytest.raises(FileExistsError):
            fs.makedirs("test_folder", exist_ok=False)

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_create_subfolder",
    )
    def test_mkdir_nested_without_parents(self, fs):
        """Test mkdir fails for nested path without create_parents"""
        with pytest.raises(FileNotFoundError):
            fs.mkdir("parent/child", create_parents=False)

    @pytest.mark.usefixtures(
        "mock_folder_get_items",
        "mock_folder_get",
        "mock_create_subfolder",
    )
    def test_mkdir_nested_with_parents(self, fs):
        """Test mkdir succeeds for nested path with create_parents"""
        fs.mkdir("parent/child", create_parents=True)

        assert fs.exists("parent")
        assert fs.exists("parent/child")
        assert fs.isdir("parent/child")

# Made with Bob
