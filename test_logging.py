def test_logging():
    import io
    import logging

    # Create string capture handlers
    main_capture = io.StringIO()
    end_tag_capture = io.StringIO()

    # Set up handlers
    main_handler = logging.StreamHandler(main_capture)
    end_tag_handler = logging.StreamHandler(end_tag_capture)

    # Set up loggers with handlers
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