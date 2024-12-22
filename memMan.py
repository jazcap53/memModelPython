import time
from typing import Dict, List, Optional
from collections import deque

from ajTypes import bNum_t, u32Const, bNum_tConst
from ajUtils import get_cur_time, Tabber
from pageTable import PageTable, PgTabEntry
from simDisk import SimDisk
from journal import Journal
from change import Change, ChangeLog
from status import Status
from myMemory import Memory, Page
from fileMan import FileMan
from arrBit import ArrBit

import os

class MemMan:
    WRITEALL_DELAY_USEC = 25000
    JRNL_PURGE_DELAY_USEC = 100000
    CHANGE_LOG_FULL = u32Const.BYTES_PER_PAGE.value * 2
    CG_OHEAD = 16
    JRNL_ENTRY_OHEAD = 24

    def __init__(self, pm: Memory, sim_disk: SimDisk, pj: Journal, pcl: ChangeLog, pstt: Status, v: bool = False):
        self.pT = PageTable()
        self.p_m = pm
        self.sim_disk = sim_disk
        self.p_j = pj
        self.p_cL = pcl
        self.p_stt = pstt
        self.verbose = v

        self.blks_in_mem = ArrBit(bNum_tConst.NUM_DISK_BLOCKS.value, u32Const.BITS_PER_PAGE.value)
        self.blk_locs_in_mem: Dict[bNum_t, int] = {}

        self.tabs = Tabber()

        # Change write timer starts now; handles case of slow program startup
        self.p_cL.last_cg_wrt_time = get_cur_time()
        self.p_stt.wrt("Running")

    def __del__(self):
        print("\n\tProgram exiting...")
        self.p_j.wrt_cg_log_to_jrnl(self.p_cL)
        self.p_j.purge_jrnl(False, False)  # False: don't keep going; program finished

    def process_request(self, cg: Change, p_fM: FileMan):
        assert len(self.pT.pg_tab) == self.p_m.num_mem_slots - sum(self.p_m.avl_mem_slts)

        b_num = cg.block_num
        assert b_num < bNum_tConst.NUM_DISK_BLOCKS.value

        in_mem = self.blks_in_mem.test(b_num)
        a_write = self.get_req_type(cg, in_mem)

        self.mk_pg_ready(b_num, in_mem)

        if a_write:
            self.wrt_in_slot(cg)
        else:
            self.rd_in_slot(b_num)

        self.timed_acts(a_write, b_num, p_fM)

    def get_req_type(self, cg: Change, in_mem: bool) -> bool:
        a_wrt = bool(cg.selectors)  # Assume it's a write if there are any selectors
        cg.time_stamp = get_cur_time()

        print(f"{self.tabs(1, True)}Request for {'write to' if a_wrt else 'read from'} "
              f"block {cg.block_num} {'in' if in_mem else 'not in'} memory at time {cg.time_stamp}")

        return a_wrt

    def mk_pg_ready(self, b_num: bNum_t, in_mem: bool):
        mem_slot = None

        if not in_mem:
            mem_slot = self.setup_pg(b_num)

        if mem_slot is not None:
            self.update_pg_in_mem(b_num, mem_slot)

    def timed_acts(self, a_write: bool, b_num: bNum_t, p_fM: FileMan):
        if self.verbose:
            self.debug_display()

        bytes_to_jrnl = self.get_sz_jrnl_wrt()
        if bytes_to_jrnl >= self.CHANGE_LOG_FULL:
            print(f"\n{self.tabs(1)}BytesToJrnl {bytes_to_jrnl} >= CG_LOG_FULL ({self.CHANGE_LOG_FULL})")

        cur_time = get_cur_time()
        elapsed = cur_time - self.p_j.last_jrnl_purge_time
        if elapsed > self.JRNL_PURGE_DELAY_USEC:
            print(f"\n\tElapsed {elapsed} > JRNL_PURGE_DELAY_USEC ({self.JRNL_PURGE_DELAY_USEC})")

        if elapsed > self.JRNL_PURGE_DELAY_USEC or bytes_to_jrnl >= self.CHANGE_LOG_FULL:
            p_fM.do_store_inodes()
            p_fM.do_store_free_list()
            self.p_cL.last_cg_wrt_time = cur_time
            self.p_j.wrt_cg_log_to_jrnl(self.p_cL)
            self.p_j.last_jrnl_purge_time = cur_time
            self.p_j.purge_jrnl(True, False)  # True to keep going, False for no crash
        else:
            delay = cur_time - self.p_cL.last_cg_wrt_time
            if delay > self.WRITEALL_DELAY_USEC:
                print(f"\n{self.tabs(1)}Delay {delay} > WRITEALL_DELAY_USEC ({self.WRITEALL_DELAY_USEC})")
                self.p_cL.last_cg_wrt_time = cur_time
                self.p_j.wrt_cg_log_to_jrnl(self.p_cL)

    def get_sz_jrnl_wrt(self) -> int:
        ttl_sz = 0
        num_data_lines = self.p_cL.get_cg_line_ct()
        data_bytes = num_data_lines * u32Const.BYTES_PER_LINE.value
        select_bytes = (num_data_lines >> 3) + 8
        ttl_sz += data_bytes + select_bytes + self.CG_OHEAD + self.JRNL_ENTRY_OHEAD
        return ttl_sz

    def setup_pg(self, b_num: bNum_t) -> Optional[int]:
        mem_slot = self.p_m.get_first_avl_mem_slt()

        if mem_slot == self.p_m.num_mem_slots:
            mem_slot = self.evict_lru_page()

        if mem_slot is not None:
            self.rd_pg_frm_dsk(b_num, mem_slot)

        return mem_slot

    def update_pg_in_mem(self, b_num: bNum_t, slot: int):
        if b_num in self.p_cL.the_log:
            lc = self.p_cL.the_log[b_num]
            for cg in lc:
                self.p_j.wrt_cg_to_pg(cg, self.p_m.get_page(slot))

    def evict_lru_page(self) -> Optional[int]:
        mem_slot = None

        if len(self.pT.pg_tab) == self.p_m.num_mem_slots:
            assert self.pT.check_heap()

            self.pT.print()

            temp = self.pT.do_pop_heap()

            old_page_num = temp.block_num
            mem_slot = self.blk_locs_in_mem[old_page_num]

            del self.blk_locs_in_mem[old_page_num]
            self.blks_in_mem.reset(old_page_num)

            self.p_m.make_avl_mem_slt(mem_slot)

            print(f"{self.tabs(1, True)}Evicted page {old_page_num} from memory slot {mem_slot} at time {get_cur_time()}")

            assert self.pT.check_heap()

        return mem_slot

    def evict_this_page(self, b_num: bNum_t):
        if self.blk_in_pg_tab(b_num):
            assert self.pT.check_heap()

            mem_slot = self.blk_locs_in_mem[b_num]
            pg_tab_slot = self.pT.get_pg_tab_slot_frm_mem_slot(mem_slot)

            assert pg_tab_slot != len(self.pT.pg_tab)

            self.pT.reset_a_time(pg_tab_slot)
            self.pT.heapify()

            dummy = self.pT.do_pop_heap()

            assert dummy.block_num == b_num

            del self.blk_locs_in_mem[b_num]
            self.blks_in_mem.reset(b_num)

            self.p_m.make_avl_mem_slt(mem_slot)

            print(f"{self.tabs(2, True)}Evicted page {b_num} from memory slot {mem_slot} at time {get_cur_time()}")

            assert self.pT.check_heap()

    def rd_pg_frm_dsk(self, b_num: bNum_t, mem_slot: int):
        assert self.pT.check_heap()

        if mem_slot == u32Const.NUM_MEM_SLOTS.value - 1:
            self.pT.set_pg_tab_full()

        print(f"{self.tabs(1)}Moving page {b_num} into memory slot {mem_slot} at time {get_cur_time()}")

        self.sim_disk.get_ds().seek(b_num * u32Const.BLOCK_BYTES.value)
        page_data = self.sim_disk.get_ds().read(u32Const.BLOCK_BYTES.value)
        self.p_m.get_page(mem_slot).dat = bytearray(page_data)

        self.blks_in_mem.set(b_num)
        self.blk_locs_in_mem[b_num] = mem_slot

        temp = PgTabEntry(b_num, mem_slot, get_cur_time())
        self.p_m.take_avl_mem_slt(mem_slot)

        self.pT.do_push_heap(temp)

        assert self.pT.check_heap()

    def rd_in_slot(self, b_num: bNum_t):
        m_slot = self.blk_locs_in_mem[b_num]
        p_t_slot = self.pT.get_pg_tab_slot_frm_mem_slot(m_slot)

        assert p_t_slot != len(self.pT.pg_tab)

        self.pT.update_a_time(p_t_slot)
        print(f"{self.tabs(1)}Reading from page {b_num} in memory slot {m_slot} at time "
              f"{self.pT.get_pg_tab_entry(p_t_slot).acc_time}")

        if not self.pT.is_leaf(p_t_slot):
            self.pT.heapify()

    def wrt_in_slot(self, cg: Change):
        assert cg.selectors

        m_slot = self.blk_locs_in_mem[cg.block_num]
        p_t_slot = self.pT.get_pg_tab_slot_frm_mem_slot(m_slot)

        b_num = cg.block_num

        self.pT.update_a_time(p_t_slot)
        print(f"{self.tabs(1)}Writing to page {b_num} in memory slot {m_slot} at time "
              f"{self.pT.get_pg_tab_entry(p_t_slot).acc_time}")

        self.p_cL.add_to_log(cg)

        if not self.pT.is_leaf(p_t_slot):
            self.pT.heapify()

    def blk_in_pg_tab(self, b_num: bNum_t) -> bool:
        return b_num in self.blk_locs_in_mem

    def debug_display(self):
        print("\nBLOCK   MSLOT   mslot   atime   ptslot")
        print("=====   =====   =====   =====   ======")
        for i in range(bNum_tConst.NUM_DISK_BLOCKS.value):
            if self.blks_in_mem.test(i):
                print(f"{i:5d}   {self.blk_locs_in_mem[i]:5d}   ", end="")
                for j in range(u32Const.NUM_MEM_SLOTS.value):
                    if self.pT.pg_tab[j].block_num == i:
                        print(f"{self.pT.pg_tab[j].mem_slot_ix:5d}   {self.pT.pg_tab[j].acc_time:5d}    {j:6d}")
                        break
        print()


def cleanup_files():
    """Remove files created during testing."""
    files_to_remove = ["mock_disk.bin", "mock_status.txt", "mock_journal.bin"]
    for file in files_to_remove:
        if os.path.exists(file):
            os.remove(file)
            print(f"Removed {file}")

def close_resources(sim_disk):
    """Close open file handles."""
    if hasattr(sim_disk, 'ds') and sim_disk.ds and not sim_disk.ds.closed:
        sim_disk.ds.close()
        print("Closed sim_disk file handle")

if __name__ == "__main__":
    # Basic test setup
    class MockMemory(Memory):
        def __init__(self):
            super().__init__()
            self.pages = [Page() for _ in range(u32Const.NUM_MEM_SLOTS.value)]
            self.NUM_MEM_SLOTS = u32Const.NUM_MEM_SLOTS.value
            self.avl_mem_slts = [True] * self.NUM_MEM_SLOTS

        def get_page(self, ix):
            return self.pages[ix]

        def get_first_avl_mem_slt(self):
            for i, is_available in enumerate(self.avl_mem_slts):
                if is_available:
                    return i
            return self.NUM_MEM_SLOTS

        def make_avl_mem_slt(self, mem_slt):
            self.avl_mem_slts[mem_slt] = True

        def take_avl_mem_slt(self, mem_slt):
            self.avl_mem_slts[mem_slt] = False

    class MockSimDisk:
        def __init__(self):
            self.ds = open("mock_disk.bin", "wb+")
            self.ds.write(b'\0' * u32Const.JRNL_SIZE.value)

        def get_ds(self):
            return self.ds

    class MockJournal:
        def __init__(self):
            self.last_jrnl_purge_time = 0

        def wrt_cg_log_to_jrnl(self, r_cg_log):
            print("Writing change log to journal")

        def purge_jrnl(self, keep_going: bool, had_crash: bool = False):
            print(f"Purging journal (keep_going={keep_going}, had_crash={had_crash})")

        def wrt_cg_to_pg(self, cg, pg):
            print(f"Writing change to page for block {cg.block_num}")

    class MockChangeLog(ChangeLog):
        def __init__(self):
            super().__init__()
            self.last_cg_wrt_time = 0

        def get_cg_line_ct(self):
            return sum(len(changes) for changes in self.the_log.values())

    class MockStatus:
        def __init__(self, filename):
            self.filename = filename

        def wrt(self, msg):
            print(f"Writing status: {msg}")

    # Create mock objects
    mock_memory = MockMemory()
    mock_sim_disk = MockSimDisk()
    mock_journal = MockJournal()
    mock_change_log = MockChangeLog()
    mock_status = MockStatus("mock_status.txt")

    try:
        # Create MemMan instance
        mem_man = MemMan(mock_memory, mock_sim_disk, mock_journal, mock_change_log, mock_status, True)

        # Test process_request
        test_change = Change(1)  # Change for block 1
        test_change.add_selector(True)  # Add a selector
        test_change.add_line(0, b'A' * u32Const.BYTES_PER_LINE.value)

        class MockFileMan:
            def do_store_inodes(self):
                print("Storing inodes")

            def do_store_free_list(self):
                print("Storing free list")

        mock_file_man = MockFileMan()

        print("Testing process_request...")
        mem_man.process_request(test_change, mock_file_man)

        # Test evict_lru_page
        print("\nTesting evict_lru_page...")
        evicted_slot = mem_man.evict_lru_page()
        print(f"Evicted slot: {evicted_slot}")

        # Test debug_display
        print("\nTesting debug_display...")
        mem_man.debug_display()

        print("MemMan tests completed.")

    finally:
        # Always close resources
        close_resources(mock_sim_disk)

        # Cleanup files (commented out by default)
        # cleanup_files()

    print("MemMan module test finished.")