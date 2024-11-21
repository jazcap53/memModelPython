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


class PrintInterceptor:
    def __init__(self, old_stdout, show_prints):
        self.old_stdout = old_stdout
        self.show_prints = show_prints

    def write(self, message):
        if 'DEBUG' in message:
            if self.show_prints:
                self.old_stdout.write(message)
        else:
            self.old_stdout.write(message)

    def flush(self):
        self.old_stdout.flush()


@pytest.fixture(autouse=True)
def control_output(request, caplog):
    """Control debug output for tests."""
    show_prints = request.config.getoption("--show-prints")
    show_logs = request.config.getoption("--show-logs")

    # Set up logging level
    if show_logs:
        caplog.set_level(logging.DEBUG)
    else:
        caplog.set_level(logging.WARNING)

    # Intercept print statements
    old_stdout = sys.stdout
    sys.stdout = PrintInterceptor(old_stdout, show_prints)

    yield

    # Restore stdout
    sys.stdout = old_stdout