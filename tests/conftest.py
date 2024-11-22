import pytest
import logging


@pytest.fixture(autouse=True)
def control_output(caplog):
    """Set default logging level."""
    caplog.set_level(logging.WARNING)
    yield