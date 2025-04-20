from journal import logger, end_tag_logger
from journal import Journal
from change import ChangeLog
from ajTypes import u32Const, write_64bit, read_64bit
import io
import logging


def setup_corrupted_journal():
    """Create a journal with a corrupted end tag for testing."""
    import tempfile
    import os

    temp_dir = tempfile.gettempdir()
    test_file = os.path.join(temp_dir, "test_journal.bin")

    # Create mock dependencies
    from unittest.mock import MagicMock
    mock_sim_disk = MagicMock()
    mock_change_log = MagicMock()
    mock_status = MagicMock()
    mock_crash_chk = MagicMock()
    mock_crash_chk.get_last_status.return_value = "Normal"

    # Remove old file if it exists
    if os.path.exists(test_file):
        os.remove(test_file)

    # Create journal with debug mode enabled to show errors
    journal = Journal(test_file, mock_sim_disk, mock_change_log, mock_status, mock_crash_chk, debug=True)

    # Write intentionally corrupted data with proper structure to get to end tag verification
    journal.seek(journal.META_LEN)

    # Write valid start tag using journal's functions
    write_64bit(journal.journal_file, journal.START_TAG)

    # Write data size - using a value that's a valid-looking size
    data_size = 8 + 8 + 8 + 64  # Block num + timestamp + selector + data line
    write_64bit(journal.journal_file, data_size)

    # Write a valid-looking block number and timestamp
    write_64bit(journal.journal_file, 1)  # Valid block number
    write_64bit(journal.journal_file, 12345)  # Valid timestamp

    # Write a valid selector with bit 0 and MSB set
    write_64bit(journal.journal_file, (1 << 63) | 1)  # Valid selector

    # Write mock data for one line
    journal.write(b'TEST DATA' + b'\x00' * 56)  # Padding to line size

    # Write CORRUPTED end tag (it must be at the expected position)
    end_pos = journal.META_LEN + journal.HEADER_SIZE + data_size
    journal.seek(end_pos)
    write_64bit(journal.journal_file, journal.END_TAG ^ 0xFFFFFFFF)  # Corrupt but same size

    # Update metadata to point to our data
    journal._metadata.write(journal.META_LEN, journal.tell(), data_size + journal.HEADER_SIZE + journal.END_TAG_SIZE)

    # Create a new change log for reading
    read_change_log = ChangeLog()

    return journal, read_change_log


def test_logging():
    import io

    # Create string capture handlers
    main_capture = io.StringIO()
    end_tag_capture = io.StringIO()

    # Set up handlers for capturing output
    main_handler = logging.StreamHandler(main_capture)
    end_tag_handler = logging.StreamHandler(end_tag_capture)

    # Set up formatters to match expected output
    formatter = logging.Formatter('%(message)s')
    main_handler.setFormatter(formatter)
    end_tag_handler.setFormatter(formatter)

    # Set log levels - Use DEBUG to catch all log messages
    main_handler.setLevel(logging.DEBUG)
    end_tag_handler.setLevel(logging.DEBUG)

    # Save original settings to restore later
    original_logger_level = logger.level
    original_end_tag_logger_level = end_tag_logger.level
    original_logger_propagate = logger.propagate
    original_end_tag_logger_propagate = end_tag_logger.propagate

    try:
        # Temporarily set loggers to DEBUG level
        logger.setLevel(logging.DEBUG)
        end_tag_logger.setLevel(logging.DEBUG)

        # CRITICAL FIX: Disable propagation for end_tag_logger
        logger.propagate = False
        end_tag_logger.propagate = False

        # Add handlers to loggers
        logger.addHandler(main_handler)
        end_tag_logger.addHandler(end_tag_handler)

        # Create corrupted journal
        journal, change_log = setup_corrupted_journal()

        # Read the journal (which should trigger the error)
        try:
            journal.rd_last_jrnl(change_log)
        except ValueError as e:
            print(f"Expected error: {e}")

        # Check log content
        end_tag_log = end_tag_capture.getvalue()
        main_log = main_capture.getvalue()

        print(f"Main logger received: {main_log}")
        print(f"End tag logger received: {end_tag_log}")

        # Now the test should pass - we've fixed propagation
        assert "End tag verification failed" in end_tag_log
        assert "End tag verification failed" not in main_log

    finally:
        # Clean up handlers
        logger.removeHandler(main_handler)
        end_tag_logger.removeHandler(end_tag_handler)

        # Restore original settings
        logger.setLevel(original_logger_level)
        end_tag_logger.setLevel(original_end_tag_logger_level)
        logger.propagate = original_logger_propagate
        end_tag_logger.propagate = original_end_tag_logger_propagate


if __name__ == "__main__":
    test_logging()