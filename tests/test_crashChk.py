import pytest
from io import StringIO
import builtins
from crashChk import CrashChk


class FileSystem:
    """A class to manage our virtual file system using StringIO objects"""

    def __init__(self):
        self.files = {}

    def write_file(self, filename: str, content: str):
        self.files[filename] = StringIO(content)

    def remove_file(self, filename: str):
        if filename in self.files:
            self.files[filename].close()
            del self.files[filename]

    def clear(self):
        for file in self.files.values():
            file.close()
        self.files.clear()

    def open(self, filename: str, mode: str):
        if filename not in self.files:
            raise FileNotFoundError(f"No such file: {filename}")
        self.files[filename].seek(0)  # Reset file pointer to start
        return self.files[filename]


@pytest.fixture
def virtual_fs():
    # Create our virtual file system
    fs = FileSystem()

    # Store the original open function
    original_open = builtins.open

    # Replace the built-in open with our virtual one
    def mock_open(filename, mode='r'):
        return fs.open(filename, mode)

    builtins.open = mock_open

    # Provide the file system for test use
    yield fs

    # Clean up and restore original open
    fs.clear()
    builtins.open = original_open


def test_no_status_files(virtual_fs):
    """Test behavior when neither status file exists"""
    checker = CrashChk()
    assert checker.get_last_status() == ""


def test_status_txt_exists(virtual_fs):
    """Test reading from status.txt when it exists"""
    expected_status = "Normal operation"
    virtual_fs.write_file("status.txt", expected_status)

    checker = CrashChk()
    assert checker.get_last_status() == expected_status


def test_status_tmp_fallback(virtual_fs):
    """Test fallback to status.tmp when status.txt doesn't exist"""
    expected_status = "Backup status"
    virtual_fs.write_file("status.tmp", expected_status)

    checker = CrashChk()
    assert checker.get_last_status() == expected_status


def test_prefer_status_txt_over_tmp(virtual_fs):
    """Test that status.txt is preferred over status.tmp when both exist"""
    status_txt_content = "Primary status"
    status_tmp_content = "Backup status"

    virtual_fs.write_file("status.txt", status_txt_content)
    virtual_fs.write_file("status.tmp", status_tmp_content)

    checker = CrashChk()
    assert checker.get_last_status() == status_txt_content


def test_empty_status_file(virtual_fs):
    """Test behavior with empty status file"""
    virtual_fs.write_file("status.txt", "")

    checker = CrashChk()
    assert checker.get_last_status() == ""


def test_whitespace_status(virtual_fs):
    """Test that whitespace is properly stripped from status"""
    status_with_whitespace = "  Running  \n"
    expected_status = "Running"

    virtual_fs.write_file("status.txt", status_with_whitespace)

    checker = CrashChk()
    assert checker.get_last_status() == expected_status


def test_file_removal(virtual_fs):
    """Test that we can remove files from our virtual filesystem"""
    virtual_fs.write_file("status.txt", "test")
    virtual_fs.remove_file("status.txt")

    checker = CrashChk()
    assert checker.get_last_status() == ""