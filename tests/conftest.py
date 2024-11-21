# tests/conftest.py

import pytest
import logging
import sys
from _pytest.logging import LogCaptureHandler


def pytest_addoption(parser):
    """Add logging-related command line options."""
    group = parser.getgroup('custom_logging')

    group.addoption(
        "--show-warnings",
        action="store_true",
        default=False,
        help="Show warning messages during tests"
    )

    group.addoption(
        "--show-info",
        action="store_true",
        default=False,
        help="Show info messages during tests"
    )

    group.addoption(
        "--show-debug",
        action="store_true",
        default=False,
        help="Show debug messages during tests"
    )


@pytest.fixture(autouse=True)
def setup_logging(request, caplog):
    """Configure logging for tests with granular control."""
    # Get command line options
    show_warnings = request.config.getoption("--show-warnings")
    show_info = request.config.getoption("--show-info")
    show_debug = request.config.getoption("--show-debug")

    # Set up log levels based on options
    if show_debug:
        caplog.set_level(logging.DEBUG)
    elif show_info:
        caplog.set_level(logging.INFO)
    elif show_warnings:
        caplog.set_level(logging.WARNING)
    else:
        caplog.set_level(logging.ERROR)

    class PrintCapture:
        def __init__(self, level):
            self.level = level

        def write(self, message):
            if message.strip():
                logging.log(self.level, message.rstrip())

        def flush(self):
            pass

    # Capture stdout and stderr
    old_stdout = sys.stdout
    old_stderr = sys.stderr

    if show_debug or show_info:
        sys.stdout = PrintCapture(logging.INFO)
    if show_warnings:
        sys.stderr = PrintCapture(logging.WARNING)

    yield

    # Restore stdout and stderr
    sys.stdout = old_stdout
    sys.stderr = old_stderr


@pytest.fixture
def assert_no_prints():
    """Fixture to ensure no print statements are used in a test."""
    old_stdout = sys.stdout
    old_stderr = sys.stderr

    class PrintCatcher:
        def __init__(self):
            self.printed = []

        def write(self, message):
            if message.strip():
                self.printed.append(message.strip())

        def flush(self):
            pass

    stdout_catcher = PrintCatcher()
    stderr_catcher = PrintCatcher()
    sys.stdout = stdout_catcher
    sys.stderr = stderr_catcher

    yield

    sys.stdout = old_stdout
    sys.stderr = old_stderr

    if stdout_catcher.printed or stderr_catcher.printed:
        pytest.fail(f"Test used print statements: {stdout_catcher.printed + stderr_catcher.printed}")