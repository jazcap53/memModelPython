"""
client.py: User Interface Simulation Module

This module simulates user interactions with the memory model system. The Client class
represents a user or process that makes requests to the file system through the FileMan
class. It implements functionality to:

1. Create and delete files
2. Add or remove blocks from files
3. Read from and write to files
4. Generate random, realistic file system requests

The Client class serves as a placeholder for a future, more comprehensive set of
classes that will make requests of the program through the FileMan class. It provides
a realistic workload for testing the memory model system.
"""

import random
import time
from typing import List, Optional, Dict
from collections import deque

from ajTypes import bNum_t, inNum_t, Line, u32Const, lNum_tConst, bNum_tConst, SENTINEL_INUM, SENTINEL_BNUM
from change import Change
from fileMan import FileMan
from driver import Driver
from ajUtils import get_cur_time


class Client:
    def __init__(self, client_id: int, file_manager: FileMan, driver: Driver):
        self.my_id = client_id
        self.p_fm = file_manager
        self.p_drvr = driver

        # Constants
        self.SHORT_RUN = 256
        self.RUN_FACTOR = 112
        self.rd_pct = 60  # Percentage of read operations
        self.lo_chooser, self.hi_chooser = 0, 99
        self.lo_page, self.hi_page = 0, bNum_tConst.NUM_DISK_BLOCKS.value - 1
        self.min_delay, self.max_delay = 0, 850
        self.actions = 99
        self.hi_inode = (u32Const.NUM_INODE_TBL_BLOCKS.value * lNum_tConst.INODES_PER_BLOCK.value - 1)

        # Initialize random number generator
        self.rng = random.Random()

        self.my_open_files: List[List[int]] = []
        self.init()

    def init(self):
        if self.p_drvr.get_test():
            # Seed with a unique repeatable value
            initial_seed = self.p_drvr.get_the_seed()
            self.rng.seed(initial_seed)  # + self.get_my_id() * initial_seed)
        else:
            # Seed with a unique pseudorandom value
            self.rng.seed(get_cur_time())

        # Set up number of requests
        if self.p_drvr.get_long_run():
            self.num_requests = self.RUN_FACTOR * u32Const.PAGES_PER_JRNL.value
        else:
            self.num_requests = self.SHORT_RUN

    def make_requests(self):
        num_requests = (self.RUN_FACTOR * u32Const.PAGES_PER_JRNL.value
                   if self.p_drvr.get_long_run()
                   else self.SHORT_RUN)
        for i in range(num_requests):
            self.rnd_delay()
            act = self.rng.randint(0, self.actions)

            if act < 5:
                self.create_or_delete()
            elif act < 6:
                self.delete_or_create()
            elif act < 20:
                self.add_rnd_block()
            elif act < 23:
                self.remv_rnd_block()
            else:
                self.make_rw_request()

    def get_my_id(self) -> int:
        return self.my_id

    # Private methods
    def create_or_delete(self):
        if self.req_count_files() < u32Const.NUM_INODE_TBL_BLOCKS.value * lNum_tConst.INODES_PER_BLOCK.value - 1:
            self.req_create_file()
        else:
            self.req_delete_file(self.rng.randint(0, self.hi_inode))

    def delete_or_create(self):
        tgt = self.rnd_file_num()
        if tgt != SENTINEL_INUM:
            self.req_delete_file(tgt)
        else:
            self.req_create_file()

    def add_rnd_block(self):
        tgt = self.rnd_file_num()
        if tgt != SENTINEL_INUM:
            if self.req_count_blocks(tgt) < u32Const.CT_INODE_BNUMS.value - 1:
                self.req_add_block(tgt)

    def remv_rnd_block(self):
        tgt_nd_num = self.rnd_file_num()
        if tgt_nd_num != SENTINEL_INUM:
            tgt_blk_num = self.rnd_blk_num(tgt_nd_num)
            if tgt_blk_num != SENTINEL_BNUM:
                self.req_remv_block(tgt_nd_num, tgt_blk_num)

    def make_rw_request(self):
        tgt_nd_num = self.rnd_file_num()
        if tgt_nd_num != SENTINEL_INUM:
            tgt_blk_num = self.rnd_blk_num(tgt_nd_num)
            if tgt_blk_num != SENTINEL_BNUM:
                rd_or_wrt = self.rng.randint(self.lo_chooser, self.hi_chooser)
                do_wrt = rd_or_wrt >= self.rd_pct
                cg = Change(tgt_blk_num)
                if do_wrt:
                    self.set_up_cgs(cg)
                self.req_submit_request(do_wrt, self.my_id, tgt_nd_num, cg)

    # File Manager request methods
    def req_create_file(self) -> inNum_t:
        return self.p_fm.create_file()

    def req_delete_file(self, i_num: inNum_t) -> bool:
        return self.p_fm.delete_file(self.my_id, i_num)

    def req_count_files(self) -> inNum_t:
        return self.p_fm.count_files()

    def req_count_blocks(self, i_num: inNum_t) -> bNum_t:
        return self.p_fm.count_blocks(i_num)

    def req_file_exists(self, i_num: inNum_t) -> bool:
        return self.p_fm.file_exists(i_num)

    def req_add_block(self, i_num: inNum_t) -> bNum_t:
        return self.p_fm.add_block(self.my_id, i_num)

    def req_remv_block(self, i_num: inNum_t, b_num: bNum_t) -> bool:
        return self.p_fm.remv_block(self.my_id, i_num, b_num)

    def req_get_inode(self, i_num: inNum_t):
        return self.p_fm.get_inode(i_num)

    def req_submit_request(self, do_wrt: bool, cli_id: int, i_num: inNum_t, cg: Change):
        self.p_fm.submit_request(do_wrt, cli_id, i_num, cg)

    # Utility methods
    def rnd_file_num(self) -> inNum_t:
        file_ct = self.req_count_files()
        if file_ct:
            while True:
                tgt = self.rng.randint(0, u32Const.NUM_INODE_TBL_BLOCKS.value * lNum_tConst.INODES_PER_BLOCK.value - 1)
                if self.req_file_exists(tgt):
                    return tgt
        return SENTINEL_INUM

    def rnd_blk_num(self, tgt_nd_num: inNum_t) -> bNum_t:
        if self.req_count_blocks(tgt_nd_num):
            tmp = self.req_get_inode(tgt_nd_num)
            valid_blocks = [b for b in tmp.b_nums if b != SENTINEL_INUM]
            if valid_blocks:
                return self.rng.choice(valid_blocks)
        return SENTINEL_BNUM

    def set_up_cgs(self, cg: Change):
        max_lines_changed = 15
        num_cgs = self.rng.randint(1, max_lines_changed)

        for i in range(num_cgs):
            if self.p_drvr.get_test() and i == 0:
                s = f"Block {cg.block_num}\n"
                lin_num = 0
            else:
                if self.p_drvr.get_test():
                    lin_num = self.rng.randint(1, lNum_tConst.LINES_PER_PAGE.value - 1)
                else:
                    lin_num = self.rng.randint(0, lNum_tConst.LINES_PER_PAGE.value - 1)
                s = f"Line {lin_num}\n"

            lin = bytearray(u32Const.BYTES_PER_LINE.value)
            self.lin_cpy(lin, s)
            cg.add_line(lin_num, lin)

    def lin_cpy(self, ln: bytearray, s: str):
        sz = len(s)

        assert sz < u32Const.BYTES_PER_LINE.value

        for i in range(u32Const.BYTES_PER_LINE.value - 1):
            ln[i] = ord(s[i]) if i < sz else 0
        ln[u32Const.BYTES_PER_LINE.value - 1] = sz

    def rnd_delay(self):
        delay = self.rng.randint(self.min_delay, self.max_delay)
        end_time = get_cur_time() + delay
        while get_cur_time() < end_time:
            pass


if __name__ == "__main__":
    # Create mock objects for testing
    class MockFileMan:
        def create_file(self) -> inNum_t:
            return 1

        def delete_file(self, client_id: int, i_num: inNum_t) -> bool:
            return True

        def count_files(self) -> inNum_t:
            return 5

        def count_blocks(self, i_num: inNum_t) -> bNum_t:
            return 3

        def file_exists(self, i_num: inNum_t) -> bool:
            return True

        def add_block(self, client_id: int, i_num: inNum_t) -> bNum_t:
            return 10

        def remv_block(self, client_id: int, i_num: inNum_t, b_num: bNum_t) -> bool:
            return True

        def get_inode(self, i_num: inNum_t):
            class MockInode:
                def __init__(self):
                    # Ensure we have CT_INODE_BNUMS elements
                    self.b_nums = [1, 2, 3] + [SENTINEL_INUM] * (u32Const.CT_INODE_BNUMS.value - 3)

            return MockInode()

        def submit_request(self, do_wrt: bool, cli_id: int, i_num: inNum_t, cg: Change):
            print(f"Request submitted: write={do_wrt}, client={cli_id}, inode={i_num}")


    # Create test driver and client
    test_args = ["memModel", "-t"]
    test_driver = Driver(test_args)
    mock_file_man = MockFileMan()
    client = Client(1, mock_file_man, test_driver)

    # Test client methods
    print("Testing client creation...")
    new_file = client.req_create_file()
    print(f"Created file with inode number: {new_file}")

    print("\nTesting file operations...")
    client.make_requests()

    print("\nClient tests completed.")