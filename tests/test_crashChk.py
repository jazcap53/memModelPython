import pytest
import os
from crashChk import CrashChk


@pytest.fixture
def cleanup_status_files():
    # Setup: ensure no status files exist at start
    for filename in ['status.txt', 'status.tmp']:
        if os.path.exists(filename):
            os.remove(filename)

    # Let the test run
    yield

    # Cleanup: remove any status files after test
    for filename in ['status.txt', 'status.tmp']:
        if os.path.exists(filename):
            os.remove(filename)


def test_no_status_files(cleanup_status_files):
    """Test behavior when neither status file exists"""
    checker = CrashChk()
    assert checker.get_last_status() == ""


def test_status_txt_exists(cleanup_status_files):
    """Test reading from status.txt when it exists"""
    expected_status = "Normal operation"
    with open("status.txt", "w") as f:
        f.write(expected_status)

    checker = CrashChk()
    assert checker.get_last_status() == expected_status


def test_status_tmp_fallback(cleanup_status_files):
    """Test fallback to status.tmp when status.txt doesn't exist"""
    expected_status = "Backup status"
    with open("status.tmp", "w") as f:
        f.write(expected_status)

    checker = CrashChk()
    assert checker.get_last_status() == expected_status


def test_prefer_status_txt_over_tmp(cleanup_status_files):
    """Test that status.txt is preferred over status.tmp when both exist"""
    status_txt_content = "Primary status"
    status_tmp_content = "Backup status"

    with open("status.txt", "w") as f:
        f.write(status_txt_content)
    with open("status.tmp", "w") as f:
        f.write(status_tmp_content)

    checker = CrashChk()
    assert checker.get_last_status() == status_txt_content


def test_empty_status_file(cleanup_status_files):
    """Test behavior with empty status file"""
    with open("status.txt", "w") as f:
        f.write("")

    checker = CrashChk()
    assert checker.get_last_status() == ""


def test_whitespace_status(cleanup_status_files):
    """Test that whitespace is properly stripped from status"""
    status_with_whitespace = "  Running  \n"
    expected_status = "Running"

    with open("status.txt", "w") as f:
        f.write(status_with_whitespace)

    checker = CrashChk()
    assert checker.get_last_status() == expected_status