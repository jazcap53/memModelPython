import pytest
from unittest.mock import mock_open, patch
from crashChk import CrashChk


@pytest.fixture
def mock_file_ops():
    # Create a mock context that will be used for all file operations
    mock = mock_open()
    with patch('builtins.open', mock):
        yield mock


def test_no_status_files(mock_file_ops):
    """Test behavior when neither status file exists"""
    # Configure mock to raise FileNotFoundError for both files
    mock_file_ops.side_effect = FileNotFoundError

    checker = CrashChk()
    assert checker.get_last_status() == ""


def test_status_txt_exists(mock_file_ops):
    """Test reading from status.txt when it exists"""
    expected_status = "Normal operation"
    mock_file_ops.return_value.readline.return_value = expected_status

    checker = CrashChk()
    assert checker.get_last_status() == expected_status.strip()
    # Verify that it tried to open status.txt
    mock_file_ops.assert_called_once_with("status.txt", "r")


def test_status_tmp_fallback(mock_file_ops):
    """Test fallback to status.tmp when status.txt doesn't exist"""
    expected_status = "Backup status"

    # Configure mock to fail for status.txt but succeed for status.tmp
    def side_effect(filename, mode):
        if filename == "status.txt":
            raise FileNotFoundError
        return mock_file_ops.return_value

    mock_file_ops.side_effect = side_effect
    mock_file_ops.return_value.readline.return_value = expected_status

    checker = CrashChk()
    assert checker.get_last_status() == expected_status.strip()
    # Verify that it tried both files in the correct order
    assert mock_file_ops.call_args_list[0][0] == ("status.txt", "r")
    assert mock_file_ops.call_args_list[1][0] == ("status.tmp", "r")


def test_prefer_status_txt_over_tmp(mock_file_ops):
    """Test that status.txt is preferred over status.tmp when both exist"""
    status_txt_content = "Primary status"
    mock_file_ops.return_value.readline.return_value = status_txt_content

    checker = CrashChk()
    assert checker.get_last_status() == status_txt_content.strip()
    # Verify that only status.txt was read
    mock_file_ops.assert_called_once_with("status.txt", "r")


def test_empty_status_file(mock_file_ops):
    """Test behavior with empty status file"""
    mock_file_ops.return_value.readline.return_value = ""

    checker = CrashChk()
    assert checker.get_last_status() == ""


def test_whitespace_status(mock_file_ops):
    """Test that whitespace is properly stripped from status"""
    status_with_whitespace = "  Running  \n"
    expected_status = "Running"
    mock_file_ops.return_value.readline.return_value = status_with_whitespace

    checker = CrashChk()
    assert checker.get_last_status() == expected_status