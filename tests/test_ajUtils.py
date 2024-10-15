import time
import pytest
from freezegun import freeze_time
from ajUtils import get_cur_time, Tabber, print_with_tabs, format_hex_like_hexdump

# Test get_cur_time
def test_get_cur_time():
    start_time = time.time()
    cur_time = get_cur_time()
    assert cur_time >= start_time * 1_000_000

@pytest.mark.parametrize("is_inode, expected", [
    (True, 1577836800000),  # 2020-01-01 00:00:00 UTC
    (False, 0)
])
def test_get_cur_time_with_inode(is_inode, expected):
    with freeze_time("2020-01-01"):
        cur_time = get_cur_time(is_inode)
        assert cur_time == expected

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
