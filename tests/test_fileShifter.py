# tests/test_fileShifter.py
import os
import logging
import pytest
import tempfile
from fileShifter import FileShifter


def test_shift_files_text_mode():
    """Test file shifting in text mode."""
    def update_file(f):
        f.write("Updated content")

    with tempfile.NamedTemporaryFile(delete=False, mode='w') as temp_orig:
        filename = temp_orig.name

    try:
        result = FileShifter.shift_files(filename, update_file)
        assert result == 0  # Successful operation

        # Verify file contents
        with open(filename, 'r') as f:
            assert f.read() == "Updated content"
    finally:
        # Clean up
        if os.path.exists(filename):
            os.unlink(filename)

def test_shift_files_binary_mode():
    """Test file shifting in binary mode."""
    def update_file(f):
        f.write(b"Updated binary content")

    with tempfile.NamedTemporaryFile(delete=False, mode='wb') as temp_orig:
        filename = temp_orig.name

    try:
        result = FileShifter.shift_files(filename, update_file, binary_mode=True)
        assert result == 0  # Successful operation

        # Verify file contents
        with open(filename, 'rb') as f:
            assert f.read() == b"Updated binary content"
    finally:
        # Clean up
        if os.path.exists(filename):
            os.unlink(filename)

def test_shift_files_error_handling():
    """Test error handling in file shifting."""
    def raise_io_error(f):
        raise IOError("Simulated file error")

    with tempfile.NamedTemporaryFile(delete=False) as temp_orig:
        filename = temp_orig.name

    try:
        result = FileShifter.shift_files(filename, raise_io_error)
        assert result != 0  # Error occurred
    finally:
        # Clean up
        if os.path.exists(filename):
            os.unlink(filename)

def test_shift_files_nonexistent_directory():
    """Test shifting files in a nonexistent directory."""
    def update_file(f):
        f.write("Updated content")

    # Use a path that's extremely unlikely to exist
    filename = "/path/to/nonexistent/directory/test_file.txt"

    result = FileShifter.shift_files(filename, update_file)
    assert result != 0  # Error should occur

def test_empty_update_function():
    """Test with an empty update function."""
    def no_op(f):
        pass

    with tempfile.NamedTemporaryFile(delete=False) as temp_orig:
        filename = temp_orig.name
        original_content = "Original content"
        temp_orig.write(original_content.encode())
        temp_orig.flush()

    try:
        result = FileShifter.shift_files(filename, no_op)
        assert result == 0  # Successful operation

        # Verify file remains unchanged
        with open(filename, 'r') as f:
            assert f.read() == original_content
    finally:
        # Clean up
        if os.path.exists(filename):
            os.unlink(filename)


def test_large_file_handling():
    """Test handling of large file content."""
    large_content = "X" * 1024 * 1024  # 1MB of data

    def update_large_file(f):
        f.write(large_content)

    with tempfile.NamedTemporaryFile(delete=False) as temp_orig:
        filename = temp_orig.name

    try:
        result = FileShifter.shift_files(filename, update_large_file)
        assert result == 0
        with open(filename, 'r') as f:
            assert f.read() == large_content
    finally:
        if os.path.exists(filename):
            os.unlink(filename)


def test_partial_update():
    """Test updating only part of an existing file."""
    original_content = "Original content"

    def partial_update(f):
        f.write("Updated")  # Only write part of file

    with tempfile.NamedTemporaryFile(delete=False, mode='w') as temp_orig:
        temp_orig.write(original_content)
        filename = temp_orig.name

    try:
        result = FileShifter.shift_files(filename, partial_update)
        assert result == 0
        with open(filename, 'r') as f:
            assert f.read() == "Updated"
    finally:
        if os.path.exists(filename):
            os.unlink(filename)


def test_readonly_original_file():
    """Test that shifting works even when original file is read-only."""
    with tempfile.NamedTemporaryFile(delete=False, mode='w') as temp_orig:
        filename = temp_orig.name
        temp_orig.write("Original")

    try:
        os.chmod(filename, 0o444)  # Make read-only
        result = FileShifter.shift_files(filename, lambda f: f.write("New content"))
        assert result == 0  # Should succeed despite read-only original

        # Verify content was updated
        with open(filename, 'r') as f:
            assert f.read() == "New content"
    finally:
        os.chmod(filename, 0o666)  # Make writable for cleanup
        if os.path.exists(filename):
            os.unlink(filename)


def test_existing_temp_file():
    """Test behavior when temporary file already exists."""

    def update_file(f):
        f.write("New content")

    with tempfile.NamedTemporaryFile(delete=False) as temp_orig:
        filename = temp_orig.name

    try:
        # Create the .tmp file beforehand
        with open(filename + ".tmp", 'w') as f:
            f.write("Existing temp content")

        result = FileShifter.shift_files(filename, update_file)
        assert result == 0  # Should succeed and overwrite temp file

        # Verify content
        with open(filename, 'r') as f:
            assert f.read() == "New content"
    finally:
        if os.path.exists(filename):
            os.unlink(filename)
        if os.path.exists(filename + ".tmp"):
            os.unlink(filename + ".tmp")


def test_exception_types():
    """Test different types of exceptions in action function."""

    def raise_value_error(f):
        raise ValueError("Test error")

    with tempfile.NamedTemporaryFile(delete=False) as temp_orig:
        filename = temp_orig.name

    try:
        result = FileShifter.shift_files(filename, raise_value_error)
        assert result != 0  # Should fail with generic error
    finally:
        if os.path.exists(filename):
            os.unlink(filename)


@pytest.mark.skipif(os.name != 'posix', reason="Requires POSIX filesystem permissions")
def test_readonly_directory():
    """Test behavior when directory is read-only."""
    with tempfile.TemporaryDirectory() as tmpdir:
        filename = os.path.join(tmpdir, "test_file")
        with open(filename, 'w') as f:
            f.write("Original")

        try:
            os.chmod(tmpdir, 0o555)  # Make directory read-only
            result = FileShifter.shift_files(filename, lambda f: f.write("New content"))
            assert result != 0  # Should fail because directory is read-only
        finally:
            os.chmod(tmpdir, 0o755)  # Make directory writable for cleanup
