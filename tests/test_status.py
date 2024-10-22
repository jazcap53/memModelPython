# tests/test_status.py
import os
import pytest
import tempfile
from status import Status


@pytest.fixture
def temp_status_file():
    """Create a temporary file for status testing."""
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
        filename = temp_file.name

    yield filename

    # Cleanup
    if os.path.exists(filename):
        os.unlink(filename)


def test_initial_write(temp_status_file):
    """Test writing an initial status message."""
    status = Status(temp_status_file)
    result = status.wrt("Initial status")
    assert result == 0

    # Verify the status was written
    with open(temp_status_file, 'r') as f:
        assert f.read().strip() == "Initial status"


def test_replace_status(temp_status_file):
    """Test replacing an existing status message."""
    status = Status(temp_status_file)

    # Write initial status
    status.wrt("First status")

    # Replace status
    result = status.wrt("Updated status")
    assert result == 0

    # Verify the status was updated
    with open(temp_status_file, 'r') as f:
        assert f.read().strip() == "Updated status"


def test_read_status(temp_status_file):
    """Test reading a status message."""
    status = Status(temp_status_file)

    # Write a status
    status.wrt("Test status message")

    # Read the status
    read_status = status.rd()
    assert read_status == "Test status message"


def test_read_nonexistent_file():
    """Test reading from a non-existent file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        nonexistent_file = os.path.join(tmpdir, "nonexistent_status.txt")
        status = Status(nonexistent_file)

        read_status = status.rd()
        assert read_status == "ERROR: Can not open status file for read in Status.rd()."


def test_read_empty_file(temp_status_file):
    """Test reading from an existing but empty status file."""
    # Create an empty file
    open(temp_status_file, 'w').close()

    status = Status(temp_status_file)
    read_status = status.rd()
    assert read_status == ""  # Expecting an empty string, not an error message


def test_write_empty_status(temp_status_file):
    """Test writing an empty status message."""
    status = Status(temp_status_file)
    result = status.wrt("")
    assert result == 0

    # Verify the empty status was written
    with open(temp_status_file, 'r') as f:
        assert f.read().strip() == ""


def test_write_long_status(temp_status_file):
    """Test writing a very long status message."""
    status = Status(temp_status_file)
    long_message = "A" * 1000
    result = status.wrt(long_message)
    assert result == 0

    # Verify the long status was written
    with open(temp_status_file, 'r') as f:
        assert f.read().strip() == long_message


def test_multiple_writes(temp_status_file):
    """Test multiple sequential writes."""
    status = Status(temp_status_file)

    messages = ["First status", "Second status", "Third status"]
    for msg in messages:
        result = status.wrt(msg)
        assert result == 0

    # Verify the last status was written
    with open(temp_status_file, 'r') as f:
        assert f.read().strip() == "Third status"


def test_status_with_special_characters(temp_status_file):
    """Test writing a status with special characters."""
    status = Status(temp_status_file)
    special_message = "Status with !@#$%^&*() special chars"
    result = status.wrt(special_message)
    assert result == 0

    # Verify the special character status was written
    with open(temp_status_file, 'r') as f:
        assert f.read().strip() == special_message


def test_write_permission_error(tmp_path):
    """Test writing to a file without write permissions."""
    # Create a read-only file
    no_write_file = tmp_path / "no_write_status.txt"
    no_write_file.touch(mode=0o444)  # Read-only permissions

    status = Status(str(no_write_file))
    result = status.wrt("Test status")

    # We expect a non-zero error code
    assert result != 0