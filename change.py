from typing import List, Dict, Deque
from collections import deque
from dataclasses import dataclass
import time

from ajTypes import bNum_t, lNum_t, Line, lNum_tConst
from ajUtils import get_cur_time

# Constants
# lNum_tConst.LINES_PER_PAGE.value = 63
BYTES_PER_LINE = 64


@dataclass
class Line:
    data: bytearray


@dataclass
class Select:
    data: List[int]

    def to_bytes(self) -> bytes:
        return bytes(self.data)


class Change:
    def __init__(self, block_num: int, push_selects: bool = True):
        self.block_num = block_num
        self.time_stamp = 0
        self.arr_next = 0
        self.push_selects = push_selects
        self.selectors: Deque[Select] = deque()
        self.new_data: Deque[Line] = deque()

        if push_selects:
            self.selectors.append(Select([0xFF] * 8))

    def add_line(self, block_num: int, line_num: int, line: Line):
        assert self.block_num == block_num
        assert line_num <= lNum_tConst.LINES_PER_PAGE.value - 1

        self.selectors[-1].data[self.arr_next] = line_num
        self.arr_next += 1
        if self.arr_next == 8:
            self.selectors.append(Select([0xFF] * 8))
            self.arr_next = 0
        self.new_data.append(line)

    def lines_altered(self) -> bool:
        return bool(self.selectors) and self.selectors[0].data[0] != 0xFF

    def print(self):
        if self.new_data:
            for line in self.new_data:
                print("\t\t", end="")
                for byte in line.data:
                    if 32 <= byte <= 126:  # printable ASCII range
                        print(chr(byte), end="")
                    else:
                        print(".", end="")
                print()
        print()

    def __lt__(self, other):
        if self.block_num == other.block_num:
            return self.time_stamp < other.time_stamp
        return self.block_num < other.block_num

class ChangeLog:
    def __init__(self, test_sw: bool = False):
        self.the_log: Dict[int, List[Change]] = {}
        self.test_sw = test_sw
        self.cg_line_ct = 0
        self.last_cg_wrt_time = 0

    def add_to_log(self, cg: Change):
        self.cg_line_ct += len(cg.new_data)

        if cg.block_num not in self.the_log:
            self.the_log[cg.block_num] = [cg]
        else:
            self.the_log[cg.block_num].append(cg)

    def print(self):
        for block_num, changes in self.the_log.items():
            for cg in changes:
                if not self.test_sw:
                    print(f"\t\t(Block {cg.block_num})")
                cg.print()

    def is_in_log(self, block_num: int) -> bool:
        return block_num in self.the_log

    def get_cg_line_ct(self) -> int:
        return self.cg_line_ct

    def get_log_size(self) -> int:
        return len(self.the_log)

if __name__ == '__main__':
    # Create a ChangeLog instance
    change_log = ChangeLog()

    # Create some Change instances and add them to the ChangeLog
    change1 = Change(1)
    change1.add_line(1, 0, Line(bytearray(b"Hello, World!")))
    change1.add_line(1, 1, Line(bytearray(b"This is a test.")))
    change_log.add_to_log(change1)

    change2 = Change(2)
    change2.add_line(2, 0, Line(bytearray(b"Another change.")))
    change_log.add_to_log(change2)

    # Print the ChangeLog
    print("ChangeLog contents:")
    change_log.print()

    # Check if a block is in the log
    print(f"Is block 1 in the log? {change_log.is_in_log(1)}")
    print(f"Is block 3 in the log? {change_log.is_in_log(3)}")

    # Get some statistics
    print(f"Total lines changed: {change_log.get_cg_line_ct()}")
    print(f"Number of blocks changed: {change_log.get_log_size()}")