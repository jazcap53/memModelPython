"""test_journal_minimal.py"""
import os
from status import Status
from simDisk import SimDisk
from change import Change, ChangeLog, Select
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
    # Add a single line of data
    test_data = b'Test data line 1\x00' + b'\x00' * (u32Const.BYTES_PER_LINE.value - 16)
    change.add_line(0, test_data)
    return change

def verify_change(change: Change):
    """Print detailed information about a change."""
    print(f"Block number: {change.block_num}")
    print(f"Timestamp: {change.time_stamp}")
    print("Selectors:")
    for i, selector in enumerate(change.selectors):
        print(f"  Selector {i}: {selector.value:016x}")
        # Print which bits are set
        set_bits = [j for j in range(64) if selector.is_set(j)]
        print(f"  Set bits: {set_bits}")
    print("Data lines:")
    print(f"Number of data lines: {len(change.new_data)}")
    for i, data in enumerate(change.new_data):
        print(f"  Line {i}: {data[:20]}")

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
            crash_chk,
            debug=True
        )

        # Create and add a simple change
        change = create_minimal_change()

        print("\nOriginal change details:")
        verify_change(change)

        change_log.add_to_log(change)

        print("\nWriting change to journal...")
        # Write the change to the journal
        journal._change_log_handler.wrt_cg_log_to_jrnl(change_log)

        # Get journal file position after write
        write_pos = journal.tell()
        print(f"Journal position after write: {write_pos}")

        print("\nReading change from journal...")
        # Create a new change log for reading
        read_log = ChangeLog(test_sw=True)

        # Record position before read
        read_start_pos = journal.tell()
        print(f"Journal position before read: {read_start_pos}")

        # CAPTURE THE ACTUAL READ POSITION
        original_seek = journal.seek

        def seek_wrapper(pos, whence=0):
            if whence == 0 and pos >= journal.META_LEN and pos < u32Const.JRNL_SIZE.value:
                print(f"!!! ACTUAL JOURNAL READ STARTING AT: {pos} !!!")
            return original_seek(pos, whence)

        journal.seek = seek_wrapper

        try:
            # Read the change back
            journal.rd_last_jrnl(read_log)
        finally:
            # Restore original seek method
            journal.seek = original_seek

        # Print journal metadata
        print(f"\nJournal Metadata:")
        print(f"meta_get: {journal.meta_get}")
        print(f"meta_put: {journal.meta_put}")
        print(f"meta_sz: {journal.meta_sz}")

        # Verify the read
        if not read_log.the_log:
            print("Error: No changes read from journal")
            return False

        if 0 not in read_log.the_log:
            print(f"Error: Block 0 not found in read log. Available blocks: {list(read_log.the_log.keys())}")
            return False

        read_change = read_log.the_log[0][0]
        print("\nRead change details:")
        verify_change(read_change)

        return True

    except Exception as e:
        print(f"Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        clean_test_files()

if __name__ == "__main__":
    success = run_minimal_test()
    print(f"\nTest {'passed' if success else 'failed'}")