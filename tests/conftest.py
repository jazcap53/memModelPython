import pytest
import logging
import sys


def pytest_addoption(parser):
    """Add custom output control options."""
    parser.addoption(
        "--show-logs",
        action="store_true",
        default=False,
        help="Show logger output"
    )


@pytest.fixture(autouse=True)
def control_output(request, caplog):
    """Control debug output for tests."""
    # Create a console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))

    # Get the root logger and add our handler
    root_logger = logging.getLogger()
    root_logger.addHandler(console_handler)

    # Set levels based on --show-logs option
    if request.config.getoption("--show-logs"):
        root_logger.setLevel(logging.DEBUG)
        console_handler.setLevel(logging.DEBUG)
    else:
        root_logger.setLevel(logging.WARNING)
        console_handler.setLevel(logging.WARNING)

    yield

    # Clean up
    root_logger.removeHandler(console_handler)