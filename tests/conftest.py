import pytest
import logging


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
    show_logs = request.config.getoption("--show-logs")

    # Set up logging level
    if show_logs:
        caplog.set_level(logging.DEBUG)
    else:
        caplog.set_level(logging.WARNING)

    yield