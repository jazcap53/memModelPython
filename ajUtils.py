import time
from typing import Callable

# Global variable to store startup time
startup = time.time()


def get_cur_time(is_inode: bool = False) -> int:
    """
    If is_inode is False:
        returns microseconds from program start
    else:
        returns milliseconds from epoch start
    """
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


# def format_hex_like_hexdump(data: bytes) -> str:
#     hex_pairs = [data[i:i+2].hex() for i in range(0, len(data), 2)]
#     return ' '.join(reversed(hex_pairs))

def format_hex_like_hexdump(data: bytes) -> str:
    return ' '.join(f"{data[i+1:i+2].hex()}{data[i:i+1].hex()}" for i in range(0, len(data), 2))