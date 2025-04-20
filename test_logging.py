from journal import logger, end_tag_logger
from journal import Journal
from change import ChangeLog
from ajTypes import u32Const


def setup_corrupted_journal():
    """Create a journal with a corrupted end tag for testing."""
    # Create a temporary journal file
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

    # Create journal
    journal = Journal(test_file, mock_sim_disk, mock_change_log, mock_status, mock_crash_chk)

    # Write intentionally corrupted data
    journal.seek(journal.META_LEN)

    # Write valid start tag
    journal.write(journal.START_TAG.to_bytes(8, byteorder='little'))

    # Write data size (32 bytes)
    journal.write((32).to_bytes(8, byteorder='little'))

    # Write mock data
    journal.write(b'\x01' * 32)

    # Write CORRUPTED end tag
    journal.write((journal.END_TAG ^ 0xFFFF).to_bytes(8, byteorder='little'))

    # Update metadata to point to our data
    journal._metadata.write(journal.META_LEN, journal.tell(), 32)

    # Create a new change log for reading
    read_change_log = ChangeLog()

    return journal, read_change_log


def test_logging():
    import io
    import logging
    from journal import logger, end_tag_logger  # Import loggers here

    # Create string capture handlers
    main_capture = io.StringIO()
    end_tag_capture = io.StringIO()

    # Set up handlers
    main_handler = logging.StreamHandler(main_capture)
    end_tag_handler = logging.StreamHandler(end_tag_capture)

    # Set up formatters to match expected output
    formatter = logging.Formatter('%(message)s')
    main_handler.setFormatter(formatter)
    end_tag_handler.setFormatter(formatter)

    # Set log levels
    main_handler.setLevel(logging.ERROR)
    end_tag_handler.setLevel(logging.ERROR)

    # Add handlers to loggers
    logger.addHandler(main_handler)
    end_tag_logger.addHandler(end_tag_handler)

    # Create corrupted journal
    journal, change_log = setup_corrupted_journal()

    try:
        journal.rd_last_jrnl(change_log)
    except:
        pass

    # Check log content
    end_tag_log = end_tag_capture.getvalue()
    main_log = main_capture.getvalue()

    print(f"Main logger received: {main_log}")
    print(f"End tag logger received: {end_tag_log}")

    assert "End tag verification failed" in end_tag_log
    assert "End tag verification failed" not in main_log

    # Clean up handlers to avoid affecting other tests
    logger.removeHandler(main_handler)
    end_tag_logger.removeHandler(end_tag_handler)


if __name__ == "__main__":
    test_logging()