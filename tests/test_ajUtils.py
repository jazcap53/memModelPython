import time
import datetime
import pytest
from freezegun import freeze_time
from ajUtils import get_cur_time, startup, Tabber, print_with_tabs, format_hex_like_hexdump

@pytest.fixture(autouse=True)
def mock_startup_time(monkeypatch):
    """Mock the startup time to a known value"""
    fake_startup = 1577836800.0  # 2020-01-01 00:00:00
    monkeypatch.setattr('ajUtils.startup', fake_startup)
    return fake_startup

def test_get_cur_time_since_startup(mock_startup_time):
    """Test microseconds since startup when is_inode is False"""
    with freeze_time("2020-01-01 00:00:01") as frozen_time:  # One second after startup
        cur_time = get_cur_time()
        # Should be 1 second (1_000_000 microseconds) since startup
        expected = 1_000_000
        assert cur_time == expected, f"Expected {expected}, got {cur_time}"

def test_get_cur_time_milliseconds_since_epoch():
    """Test milliseconds since epoch when is_inode is True"""
    with freeze_time("2020-01-01 00:00:00") as frozen_time:
        cur_time = get_cur_time(is_inode=True)
        # 2020-01-01 00:00:00 in milliseconds since epoch
        expected = 1577836800000
        assert cur_time == expected, f"Expected {expected}, got {cur_time}"

        # Advance time by 1 second and check again
        frozen_time.tick(datetime.timedelta(seconds=1))
        cur_time = get_cur_time(is_inode=True)
        expected = 1577836801000  # One second later in milliseconds
        assert cur_time == expected, f"Expected {expected}, got {cur_time}"

def test_get_cur_time_consistency(mock_startup_time):
    """Test that time differences are consistent"""
    with freeze_time("2020-01-01 00:00:00") as frozen_time:
        time1 = get_cur_time()
        frozen_time.tick(datetime.timedelta(microseconds=500000))  # Advance by 0.5 seconds
        time2 = get_cur_time()
        assert time2 - time1 == 500000, f"Expected difference of 500000, got {time2 - time1}"

# Test Tabber
def test_tabber():
    tabber = Tabber()
    assert tabber(2) == "\t\t"
    assert tabber(4) == "\t\t\t\t"
    assert tabber(2, True) == "\n\t\t"

# Test format_hex_like_hexdump
@pytest.mark.parametrize("data, expected", [
    (b"\x12\x34\x56\x78", "3412 7856"),
    (b"\xab\xcd\xef", "cdab ef"),
    (b"", "")
])
def test_format_hex_like_hexdump(data, expected):
    assert format_hex_like_hexdump(data) == expected

def test_print_with_tabs(capsys):
    print_with_tabs("Hello", "World")
    captured = capsys.readouterr()
    assert captured.out == "\t\tHello World\n"
