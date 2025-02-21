"""test_journal_multiple.py"""
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


def create_changes():
    """Create a sequence of changes to the same block."""
    changes = []

    # First change - write to first line
    change1 = Change(0)
    test_data1 = b'First change\x00' + b'\x00' * (u32Const.BYTES_PER_LINE.value - 12)
    change1.add_line(0, test_data1)
    changes.append(change1)

    # Second change - write to second line
    change2 = Change(0)
    test_data2 = b'Second change' + b'\x00' * (u32Const.BYTES_PER_LINE.value - 12)
    change2.add_line(1, test_data2)
    changes.append(change2)

    # Third change - write to third line
    change3 = Change(0)
    test_data3 = b'Third change\x00' + b'\x00' * (u32Const.BYTES_PER_LINE.value - 12)
    change3.add_line(2, test_data3)
    changes.append(change3)

    return changes


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
    print()


def run_multiple_test():
    """Run a test with multiple changes to the same block."""
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

        # Create and write multiple changes
        changes = create_changes()

        print("\nOriginal changes:")
        for i, change in enumerate(changes, 1):
            print(f"Change {i}:")
            verify_change(change)
            change_log.add_to_log(change)

        print(f"Journal position before write: {journal.tell()}")
        print("\nWriting changes to journal...")
        # Write all changes to the journal
        journal._change_log_handler.wrt_cg_log_to_jrnl(change_log)
        print(f"Journal position after write: {journal.tell()}")

        print("\nReading changes from journal...")
        # Create a new change log for reading
        read_log = ChangeLog(test_sw=True)

        # Record position before read
        read_start_pos = journal.tell()
        print(f"Journal position before read: {read_start_pos}")

        # CAPTURE THE ACTUAL READ POSITION
        original_seek = journal.seek
        actual_read_pos = None

        def seek_wrapper(pos, whence=0):
            nonlocal actual_read_pos
            if whence == 0 and pos >= journal.META_LEN and pos < u32Const.JRNL_SIZE.value:
                if actual_read_pos is None:  # Only capture the first seek
                    actual_read_pos = pos
                    print(f"!!! ACTUAL JOURNAL READ STARTING AT: {pos} !!!")
            return original_seek(pos, whence)

        journal.seek = seek_wrapper

        try:
            # Read the changes back
            journal.rd_last_jrnl(read_log)
        finally:
            # Restore original seek method
            journal.seek = original_seek

        # Print journal metadata
        print(f"\nJournal Metadata:")
        print(f"meta_get: {journal.meta_get}")
        print(f"meta_put: {journal.meta_put}")
        print(f"meta_sz: {journal.meta_sz}")

        # Verify the reads
        if not read_log.the_log:
            print("Error: No changes read from journal")
            return False

        if 0 not in read_log.the_log:
            print(f"Error: Block 0 not found in read log. Available blocks: {list(read_log.the_log.keys())}")
            return False

        print("\nRead changes:")
        for i, change in enumerate(read_log.the_log[0], 1):
            print(f"Change {i}:")
            verify_change(change)

        # Compare number of changes
        original_count = len(changes)
        read_count = len(read_log.the_log[0])
        print(f"\nNumber of changes - Original: {original_count}, Read: {read_count}")

        # Debug: dump the content of the journal file for examination
        print("\nDumping portion of journal file for analysis:")
        journal.seek(journal.META_LEN)
        journal_data = journal.read(200)  # Read first 200 bytes after metadata
        print(f"Journal data hex dump:")
        for i in range(0, len(journal_data), 16):
            chunk = journal_data[i:i+16]
            hex_values = ' '.join(f'{b:02x}' for b in chunk)
            ascii_repr = ''.join(chr(b) if 32 <= b < 127 else '.' for b in chunk)
            print(f"{i:04x}: {hex_values:<47} {ascii_repr}")

        return original_count == read_count

    except Exception as e:
        print(f"Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        clean_test_files()


if __name__ == "__main__":
    success = run_multiple_test()
    print(f"\nTest {'passed' if success else 'failed'}")