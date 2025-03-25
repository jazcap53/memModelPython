"""test_journal_multiple.py"""
import os
from status import Status
from simDisk import SimDisk
from change import Change, ChangeLog
from crashChk import CrashChk
from journal import Journal
from ajTypes import u32Const, from_bytes_64bit


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


def dump_journal_file(journal, title, start_pos=0, length=400):
    """Dump the contents of the journal file for analysis."""
    original_pos = journal.tell()

    print(f"\n{title}:")
    journal.seek(start_pos)
    journal_data = journal.read(length)

    print(f"Journal data hex dump (starting at position {start_pos}):")
    for i in range(0, len(journal_data), 16):
        chunk = journal_data[i:i + 16]
        hex_values = ' '.join(f'{b:02x}' for b in chunk)
        ascii_repr = ''.join(chr(b) if 32 <= b < 127 else '.' for b in chunk)
        offset = start_pos + i
        print(f"{offset:04x}: {hex_values:<47} {ascii_repr}")

    # Check metadata (without redundant seeks)
    journal.seek(0)
    meta_get_bytes = journal.read(8)
    meta_put_bytes = journal.read(8)
    meta_sz_bytes = journal.read(8)
    meta_get = from_bytes_64bit(meta_get_bytes)
    meta_put = from_bytes_64bit(meta_put_bytes)
    meta_sz = from_bytes_64bit(meta_sz_bytes)
    print(f"Metadata:")
    print(f"  meta_get: {meta_get}")
    print(f"  meta_put: {meta_put}")
    print(f"  meta_sz: {meta_sz}")

    # Check start tag
    journal.seek(journal.META_LEN)
    start_tag_bytes = journal.read(8)
    start_tag = from_bytes_64bit(start_tag_bytes)
    print(f"START_TAG at position {journal.META_LEN}: 0x{start_tag:016x} (Expected: 0x{journal.START_TAG:016x})")

    # Check bytes count
    bytes_count_bytes = journal.read(8)
    bytes_count = from_bytes_64bit(bytes_count_bytes)
    print(f"Bytes count at position {journal.META_LEN + 8}: {bytes_count} bytes")

    # Check end tag if available
    if hasattr(journal, 'end_tag_posn') and journal.end_tag_posn is not None:
        journal.seek(journal.end_tag_posn)
        end_tag_bytes = journal.read(8)
        if len(end_tag_bytes) == 8:
            end_tag = from_bytes_64bit(end_tag_bytes)
            print(f"END_TAG at position {journal.end_tag_posn}: 0x{end_tag:016x} (Expected: 0x{journal.END_TAG:016x})")
        else:
            print(f"Could not read 8 bytes for END_TAG at position {journal.end_tag_posn}")

    # Restore original position
    journal.seek(original_pos)


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
        journal.write_change_log_to_journal(change_log)
        print(f"Journal position after write: {journal.tell()}")

        # DUMP JOURNAL FILE AFTER WRITING
        dump_journal_file(journal, "Journal content AFTER WRITING")

        print("\nReading changes from journal...")
        # Create a new change log for reading
        read_log = ChangeLog(test_sw=True)

        # Record position before read
        read_start_pos = journal.tell()
        print(f"Journal position before read: {read_start_pos}")

        # Store the calculated end tag position
        journal.ct_bytes_to_write = journal._change_log_handler.calculate_ct_bytes_to_write(change_log)
        expected_end_tag_pos = journal.calculate_end_tag_position(
            journal.META_LEN,  # Start at META_LEN
            journal.ct_bytes_to_write
        )
        journal.end_tag_posn = expected_end_tag_pos
        print(f"JOURNAL: End tag should be at position {journal.end_tag_posn}")

        # CAPTURE THE ACTUAL READ POSITION with improved context
        original_seek = journal.seek

        def seek_wrapper(pos, whence=0):
            current_pos = journal.tell()

            # Only log meaningful journal read operations
            if whence == 0:
                if pos == 0:
                    print(f"JOURNAL: Reading metadata from position 0")
                elif pos == journal.META_LEN:
                    print(f"JOURNAL: Positioning to start of journal data section (position {pos})")
                elif pos == journal.meta_get:
                    print(f"JOURNAL: Positioning to most recent journal entry at {pos}")
                elif pos == journal.meta_put:
                    print(f"JOURNAL: Positioning to end of journal data at {pos}")
                elif journal.meta_get <= pos < journal.meta_put:
                    # Identify what journal component is being read
                    offset = pos - journal.meta_get
                    if offset == 0:
                        print(f"JOURNAL: Reading START_TAG at position {pos}")
                    elif offset == 8:
                        print(f"JOURNAL: Reading ct_bytes_to_write field at position {pos}")
                    elif offset == 16:
                        print(f"JOURNAL: Reading change data starting at position {pos}")
                    elif pos == journal.end_tag_posn:
                        print(f"JOURNAL: Reading END_TAG at position {pos}")
                    else:
                        print(f"JOURNAL: Seeking within change data to position {pos}")

            return original_seek(pos, whence)

        journal.seek = seek_wrapper

        try:
            # Read the changes back
            journal.rd_last_jrnl(read_log)
        finally:
            # Restore original seek method
            journal.seek = original_seek

        # DUMP JOURNAL FILE AFTER READING
        dump_journal_file(journal, "Journal content AFTER READING")

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