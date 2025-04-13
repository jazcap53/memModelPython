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

def create_padded_data(text, total_length=u32Const.BYTES_PER_LINE.value):
    """Create data with proper padding to ensure exact length."""
    if isinstance(text, str):
        text = text.encode('utf-8')

    # Calculate padding needed
    padding_length = total_length - len(text)
    if padding_length < 0:
        # If text is too long, truncate it
        return text[:total_length]

    # Add padding to reach exact length
    return text + b'\x00' * padding_length

def create_minimal_change():
    """Create a simple change for testing."""
    change = Change(0)  # Write to block 0
    test_data = create_padded_data(b'Test data line 1\x00')
    print(f"Test data length: {len(test_data)} bytes")  # Verification
    change.add_line(0, test_data)
    return change

def run_minimal_test():
    """Run a minimal test of journal write and read operations."""
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
    journal.write_change_log_to_journal(change_log)

    # Create a new change log for reading
    read_log = ChangeLog(test_sw=True)

    # Save the end tag position before reading
    end_tag_pos = journal.end_tag_posn

    # Read the change back
    print("Reading...")
    journal.rd_last_jrnl(read_log)

    # Verify the end tag by seeking to its known position
    journal.seek(end_tag_pos)
    end_tag = journal._file_io.read_end_tag()
    if end_tag != journal.END_TAG:
        raise ValueError(f"End tag verification failed. Expected {journal.END_TAG:x}, got {end_tag:x}")
    else:
        print(f"End tag verification successful at position {end_tag_pos}")

    # Verify reading worked
    if not read_log.the_log:
        print("Error: No changes read from journal")
        return False

    if 0 not in read_log.the_log:
        print(f"Error: Block 0 not found in read log")
        return False

    read_change = read_log.the_log[0][0]
    print(f"Successfully read back change for block {read_change.block_num}")

    return True


if __name__ == "__main__":
    success = run_minimal_test()
    print(f"Test {'passed' if success else 'failed'}")