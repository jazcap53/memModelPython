# test_journal_error_handling.py
import pytest
import os
import tempfile
from journal import Journal
from change import ChangeLog
from ajTypes import u32Const, write_64bit


def setup_corrupted_journal():
    """Create a journal with corrupted end tag."""
    # Clean up any existing test files
    test_files = ['test_disk.bin', 'test_journal.bin', 'test_free.bin', 'test_inode.bin', 'test_status.txt']
    for file in test_files:
        if os.path.exists(file):
            os.remove(file)

    # Create journal components
    status = Status('test_status.txt')
    sim_disk = SimDisk(status, 'test_disk.bin', 'test_journal.bin', 'test_free.bin', 'test_inode.bin')
    change_log = ChangeLog(test_sw=True)
    crash_chk = CrashChk()

    # Create journal
    journal = Journal('test_journal.bin', sim_disk, change_log, status, crash_chk)

    # Write corrupted data
    journal.seek(journal.META_LEN)
    journal.write(journal.START_TAG.to_bytes(8, byteorder='little'))  # Valid start tag
    journal.write((32).to_bytes(8, byteorder='little'))  # Size field
    journal.write(b'\x00' * 32)  # Dummy data
    journal.write(b'\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF')  # Corrupted end tag

    # Update metadata
    journal._metadata.write(journal.META_LEN, journal.tell(), 32)

    return journal, change_log


def test_error_propagation(monkeypatch):
    """Test that errors are properly propagated from lower-level methods."""
    # Create temporary journal file
    temp_file = tempfile.mktemp(suffix=".bin")

    # Create minimalist mocks using simple objects
    class MockObject:
        def get_ds(self):
            return self

        def read(self, size):
            return b'\x00' * size

        def get_last_status(self):
            return "Normal"

        def wrt(self, message):
            pass

    mock_sim_disk = MockObject()
    mock_change_log = ChangeLog()
    mock_status = MockObject()
    mock_crash_chk = MockObject()

    try:
        # Create journal instance
        journal = Journal(temp_file, mock_sim_disk, mock_change_log, mock_status, mock_crash_chk)

        # Setup: Write valid header so the function proceeds to call _read_changes
        journal.seek(journal.META_LEN)
        write_64bit(journal.journal_file, journal.START_TAG)
        write_64bit(journal.journal_file, 32)  # Non-zero data size to force read

        # Update metadata with non-zero size to ensure _read_changes gets called
        journal._metadata.write(journal.META_LEN, journal.META_LEN + 16, 32)

        # Create a function that will raise the test exception
        def failing_read_changes(*args, **kwargs):
            raise ValueError("Test exception")

        # Patch _read_changes with our failing function
        monkeypatch.setattr(journal, '_read_changes', failing_read_changes)

        # Test that rd_last_jrnl propagates the exception
        with pytest.raises(ValueError, match="Test exception"):
            journal.rd_last_jrnl(ChangeLog())

        # Test that rd_jrnl also propagates the exception
        with pytest.raises(ValueError, match="Test exception"):
            journal.rd_jrnl(ChangeLog(), journal.META_LEN)

    finally:
        # Clean up temp file
        if os.path.exists(temp_file):
            os.remove(temp_file)


if __name__ == "__main__":
    success = test_error_propagation()
    print(f"Test {'passed' if success else 'failed'}")