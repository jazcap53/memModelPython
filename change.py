from typing import List, Dict, Deque
from collections import deque
from dataclasses import dataclass
import time

from ajTypes import bNum_t, lNum_t, Line, lNum_tConst
from ajTypes import to_bytes_64bit, from_bytes_64bit
from ajUtils import get_cur_time


@dataclass
class Line:
    data: bytearray


class Select:
    def __init__(self):
        self.value = 0

    def set(self, line_num: int):
        if 0 <= line_num < 63:
            self.value |= (1 << line_num)
        elif line_num == 63:
            self.value |= (1 << 63)  # Set the MSb for last block flag
        else:
            raise ValueError("Invalid line number")

    def is_set(self, line_num: int) -> bool:
        if 0 <= line_num < 64:
            return bool(self.value & (1 << line_num))
        else:
            raise ValueError("Invalid line number")

    def is_last_block(self) -> bool:
        return bool(self.value & (1 << 63))

    def set_last_block(self):
        self.value |= (1 << 63)

    def to_bytes(self) -> bytes:
        return to_bytes_64bit(self.value)

    @classmethod
    def from_bytes(cls, b: bytes):
        selector = cls()
        selector.value = from_bytes_64bit(b)
        return selector

class Change:
    def __init__(self, block_num: bNum_t):
        self.block_num = block_num
        self.time_stamp = get_cur_time()
        self.selectors = deque()
        self.new_data = deque()
        self.arr_next = 0

    def add_selector(self, is_last: bool):
        selector = Select()
        if is_last:
            selector.set_last_block()
        self.selectors.append(selector)

    def add_line(self, line_num: int, data: bytes):
        if not self.selectors or self.selectors[-1].is_set(line_num % 63):
            new_selector = Select()
            if not self.selectors:
                new_selector.set_last_block()  # Set last block flag for the first (and only) selector
            self.selectors.append(new_selector)
        self.selectors[-1].set(line_num % 63)
        self.new_data.append(data)

    def is_last_block(self) -> bool:
        return any(selector.is_last_block() for selector in self.selectors)

    def __lt__(self, other):
        return self.time_stamp < other.time_stamp

    def print(self):
        print(f"\t\tBlock: {self.block_num}")
        print(f"\t\tTimestamp: {self.time_stamp}")
        print("\t\tSelectors:")
        for i, selector in enumerate(self.selectors):
            print(f"\t\t  Selector {i}: {selector.value:016x}")
        print("\t\tData:")
        for i, data in enumerate(self.new_data):
            print(f"\t\t  Line {i}: {data[:16]}...")  # Print first 16 bytes of each data line


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
    change1.add_line(0, b"Hello, World!")
    change1.add_line(1, b"This is a test.")
    change_log.add_to_log(change1)

    change2 = Change(2)
    change2.add_line(0, b"Another change.")
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