import time
from typing import Callable

# Global variables
startup = time.time()
mock_time = 0
_test_mode = False


def set_test_mode(mode: bool):
    """
    Set the global test mode flag.

    :param mode: True to enable test mode, False to disable
    """
    global _test_mode
    _test_mode = mode


def is_test_mode() -> bool:
    """
    Check if the program is running in test mode.

    :return: True if in test mode, False otherwise
    """
    return _test_mode


def get_mock_time() -> int:
    """
    Generate a mock time value that increments with each call.
    Used for deterministic time-based operations in test mode.

    :return: An integer representing mock microseconds
    """
    global mock_time
    mock_time += 1
    return mock_time


def get_cur_time(is_inode: bool = False) -> int:
    """
    Get the current time.

    In test mode:
        Returns a deterministic, incrementing mock time value.
    In normal mode:
        If is_inode is False:
            Returns microseconds from program start
        If is_inode is True:
            Returns milliseconds from epoch start

    :param is_inode: Flag to determine time calculation method in normal mode
    :return: An integer representing time in microseconds or milliseconds
    """
    if is_test_mode():
        return get_mock_time()

    if is_inode:
        return int(time.time() * 1000)  # milliseconds since epoch
    else:
        return int((time.time() - startup) * 1_000_000)  # microseconds since program start


class Tabber:
    num_tabs = 0

    @staticmethod
    def __call__(num: int, newline: bool = False) -> str:
        s = ""
        if num != Tabber.num_tabs:
            if newline:
                s += '\n'
            Tabber.num_tabs = num

        s += '\t' * Tabber.num_tabs
        return s


tabber = Tabber()


# Custom print function that uses the tabber
def print_with_tabs(*args, **kwargs):
    print(tabber(Tabber.num_tabs), end="")
    print(*args, **kwargs)


def format_hex_like_hexdump(data: bytes) -> str:
    return ' '.join(f"{data[i + 1:i + 2].hex()}{data[i:i + 1].hex()}" for i in range(0, len(data), 2))