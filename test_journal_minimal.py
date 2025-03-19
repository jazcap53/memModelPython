"""test_journal_minimal.py"""
import os
from status import Status
from simDisk import SimDisk
from change import Change, ChangeLog
from crashChk import CrashChk
from journal import Journal
from ajTypes import u32Const

def clean_test_files():
    """Remove test files if they exist."""
    test_files = [
        'test_disk.bin',
        'test_journal.bin',
        'test_free.bin',
        'test_inode.bin',
        'test_status.txt'
    ]
    for file in test_files:
        if os.path.exists(file):
            os.remove(file)

def create_minimal_change():
    """Create a simple change for testing."""
    change = Change(0)  # Write to block 0
    test_data = b'Test data line 1\x00' + b'\x00' * (u32Const.BYTES_PER_LINE.value - 16)
    change.add_line(0, test_data)
    return change

def run_minimal_test():
    """Run a minimal test of journal write and read operations."""
    try:
        clean_test_files()

        # Create required components
        status = Status('test_status.txt')
        sim_disk = SimDisk(
            status,
            'test_disk.bin',
            'test_journal.bin',
            'test_free.bin',
            'test_inode.bin'
        )
        change_log = ChangeLog(test_sw=True)
        crash_chk = CrashChk()

        # Create journal
        journal = Journal(
            'test_journal.bin',
            sim_disk,
            change_log,
            status,
            crash_chk
        )

        # Create and add a simple change
        change = create_minimal_change()
        change_log.add_to_log(change)

        # Write the change to the journal
        print("Writing...")
        journal._change_log_handler.wrt_cg_log_to_jrnl(change_log)

        # Create a new change log for reading
        read_log = ChangeLog(test_sw=True)

        # Read the change back
        print("Reading...")
        journal.rd_last_jrnl(read_log)

        # Verify the end tag
        if journal._file_io.read_end_tag() != journal.END_TAG:
            raise ValueError("End tag verification failed")

        return True

    except Exception as e:
        print(f"Test failed with error: {e}")
        return False

    finally:
        clean_test_files()

if __name__ == "__main__":
    success = run_minimal_test()
    print("Test passed" if success else "Test failed")