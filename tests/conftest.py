# tests/conftest.py

import pytest
import logging
import sys

def pytest_addoption(parser):
    """Add custom output control options."""
    group = parser.getgroup('output_control')

    group.addoption(
        "--show-prints",
        action="store_true",
        default=False,
        help="Show print debug statements"
    )

    group.addoption(
        "--show-logs",
        action="store_true",
        default=False,
        help="Show logger output"
    )

def pytest_configure(config):
    """Configure custom markers."""
    config.addinivalue_line(
        "markers",
        "no_debug_output: mark test to suppress its debug output"
    )

class PrintCapture:
    def __init__(self, show_prints):
        self.show_prints = show_prints
        self.original_write = sys.stdout.write

    def write(self, message):
        if 'DEBUG' in message and self.show_prints:
            self.original_write(f"PRINT: {message}")
        elif 'DEBUG' not in message:
            self.original_write(message)

    def flush(self):
        sys.stdout.flush()

@pytest.fixture(autouse=True)
def control_output(request):
    """Control debug output for tests."""
    show_prints = request.config.getoption("--show-prints")
    show_logs = request.config.getoption("--show-logs")

    # Check if test is marked to suppress debug output
    if request.node.get_closest_marker('no_debug_output'):
        show_prints = False
        show_logs = False

    # Handle print statements
    old_stdout = sys.stdout
    sys.stdout = PrintCapture(show_prints)

    # Handle logging
    logger = logging.getLogger()
    old_level = logger.level
    if not show_logs:
        logger.setLevel(logging.WARNING)
    else:
        logger.setLevel(logging.DEBUG)

    yield

    # Restore original stdout and logging level
    sys.stdout = old_stdout
    logger.setLevel(old_level)# tests/conftest.py

import pytest
import logging
import sys

def pytest_addoption(parser):
    """Add custom output control options."""
    group = parser.getgroup('output_control')

    group.addoption(
        "--show-prints",
        action="store_true",
        default=False,
        help="Show print debug statements"
    )

    group.addoption(
        "--show-logs",
        action="store_true",
        default=False,
        help="Show logger output"
    )

def pytest_configure(config):
    """Configure custom markers."""
    config.addinivalue_line(
        "markers",
        "no_debug_output: mark test to suppress its debug output"
    )

class PrintCapture:
    def __init__(self, show_prints):
        self.show_prints = show_prints
        self.original_write = sys.stdout.write

    def write(self, message):
        if 'DEBUG' in message and self.show_prints:
            self.original_write(f"PRINT: {message}")
        elif 'DEBUG' not in message:
            self.original_write(message)

    def flush(self):
        sys.stdout.flush()

@pytest.fixture(autouse=True)
def control_output(request):
    """Control debug output for tests."""
    show_prints = request.config.getoption("--show-prints")
    show_logs = request.config.getoption("--show-logs")

    # Check if test is marked to suppress debug output
    if request.node.get_closest_marker('no_debug_output'):
        show_prints = False
        show_logs = False

    # Handle print statements
    old_stdout = sys.stdout
    sys.stdout = PrintCapture(show_prints)

    # Handle logging
    logger = logging.getLogger()
    old_level = logger.level
    if not show_logs:
        logger.setLevel(logging.WARNING)
    else:
        logger.setLevel(logging.DEBUG)

    yield

    # Restore original stdout and logging level
    sys.stdout = old_stdout
    logger.setLevel(old_level)