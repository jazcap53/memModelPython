# test_journal_error_handling.py
from journal import Journal
from status import Status
from simDisk import SimDisk
from change import ChangeLog
from crashChk import CrashChk
from ajTypes import u32Const
import os


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


def test_error_propagation():
    """Test that errors are properly propagated."""
    journal, change_log = setup_corrupted_journal()

    try:
        # Should raise an exception due to invalid end tag
        journal.rd_last_jrnl(change_log)
        print("❌ FAILED: Expected exception was not raised")
        return False
    except ValueError as e:
        if "Invalid end tag" in str(e):
            print("✓ SUCCESS: Error correctly identified and propagated")
            return True
        else:
            print(f"❌ FAILED: Wrong error message: {e}")
            return False
    except Exception as e:
        print(f"❌ FAILED: Wrong exception type: {type(e)}")
        return False
    finally:
        # Clean up
        for file in ['test_disk.bin', 'test_journal.bin', 'test_free.bin', 'test_inode.bin', 'test_status.txt']:
            if os.path.exists(file):
                os.remove(file)


if __name__ == "__main__":
    success = test_error_propagation()
    print(f"Test {'passed' if success else 'failed'}")