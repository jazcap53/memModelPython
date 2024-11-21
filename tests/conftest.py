# tests/conftest.py

import pytest
import logging
import sys


def pytest_addoption(parser):
    """Add logging-related command line options."""
    group = parser.getgroup('logging')

    try:
        group.addoption(
            "--custom-log-level",  # Changed from --log-level
            action="store",
            default="ERROR",
            help="Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)"
        )
    except Exception as e:
        print(f"Warning: Could not add --custom-log-level option: {e}")

    try:
        group.addoption(
            "--enable-custom-logs",  # Changed from --enable-logs
            action="store_true",
            default=False,
            help="Enable custom logging output during tests"
        )
    except Exception as e:
        print(f"Warning: Could not add --enable-custom-logs option: {e}")


def pytest_configure(config):
    """Add custom pytest markers."""
    config.addinivalue_line(
        "markers",
        "uses_print: mark test that still uses print statements"
    )


class LoggerWriter:
    """Class to redirect print statements to logging."""

    def __init__(self, level):
        self.level = level

    def write(self, message):
        if message != '\n':
            self.level(message.rstrip())

    def flush(self):
        pass


@pytest.fixture(autouse=True)
def setup_logging(request):
    """Configure logging for tests and capture print statements."""
    # Get command line options, using new option names
    try:
        log_level = request.config.getoption("--custom-log-level")
    except ValueError:
        log_level = "ERROR"  # Default if option doesn't exist

    try:
        enable_logs = request.config.getoption("--enable-custom-logs")
    except ValueError:
        enable_logs = False  # Default if option doesn't exist

    # Setup logging
    logging.basicConfig(level=getattr(logging, log_level))

    # Disable all loggers by default unless --enable-custom-logs is used
    if not enable_logs:
        logging.disable(logging.CRITICAL)

    # Capture print statements
    old_stdout = sys.stdout
    old_stderr = sys.stderr

    sys.stdout = LoggerWriter(logging.info)
    sys.stderr = LoggerWriter(logging.warning)

    yield

    # Restore stdout and stderr
    sys.stdout = old_stdout
    sys.stderr = old_stderr

    # Re-enable logging
    logging.disable(logging.NOTSET)


@pytest.fixture
def enable_logging():
    """Fixture to enable logging for specific tests."""
    # Store current logging state
    previous_state = logging.root.manager.disable

    # Enable logging
    logging.disable(logging.NOTSET)

    yield

    # Restore logging state
    logging.disable(previous_state)