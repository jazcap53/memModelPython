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


# temporary test
# def test_logging_setup():
#     """Test that logging is working correctly."""
#     logger = logging.getLogger('fileShifter')
#     print(f"Logger disabled: {logger.disabled}")
#     print(f"Logger level: {logger.level}")
#     print(f"Logger effective level: {logger.getEffectiveLevel()}")
#     print(f"Logger handlers: {logger.handlers}")
#     print(f"Root logger level: {logging.getLogger().level}")
#     print(f"Root logger handlers: {logging.getLogger().handlers}")
#
#     # Try to generate a log message
#     logger.error("Test error message")