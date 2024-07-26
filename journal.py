import struct
from typing import List, Dict, Tuple
from collections import deque
from ajTypes import bNum_t, lNum_t, u32Const, bNum_tConst, SENTINEL_BNUM, SENTINEL_INUM
from ajCrc import BoostCRC
from ajUtils import get_cur_time, Tabber
from wipeList import WipeList
from change import Change, ChangeLog, Select
from myMemory import Page
import os


class Journal:
    START_TAG = 17406841880640449871
    END_TAG = 4205560943366639022
    META_LEN = 24
    NUM_PGS_JRNL_BUF = 16

    def __init__(self, f_name: str, sim_disk, change_log, status, crash_chk):
        self.f_name = f_name
        self.p_d = sim_disk
        self.p_cL = change_log
        self.p_stt = status
        self.p_cck = crash_chk

        # Initialize the file if it doesn't exist or is too small
        if not os.path.exists(self.f_name):
            # File doesn't exist, create it and fill with zeros
            with open(self.f_name, "wb") as f:
                f.write(b'\0' * u32Const.JRNL_SIZE.value)
        else:
            # File exists, check its size and extend if necessary
            with open(self.f_name, "r+b") as f:
                current_size = f.seek(0, 2)  # Move to the end and get position
                if current_size < u32Const.JRNL_SIZE.value:
                    f.write(b'\0' * (u32Const.JRNL_SIZE.value - current_size))

        # Open the file in read-write mode
        self.js = open(self.f_name, "r+b")

        # Rest of the initialization code remains the same
        self.p_buf = [None] * self.NUM_PGS_JRNL_BUF
        self.meta_get = 0
        self.meta_put = 0
        self.meta_sz = 0
        self.ttl_bytes = 0
        self.orig_p_pos = 0
        self.final_p_pos = 0
        self.sz = 8  # sizeof(select_t)
        self.sz_ul = 8
        self.blks_in_jrnl = [False] * bNum_tConst.NUM_DISK_BLOCKS.value
        self.last_jrnl_purge_time = 0
        self.tabs = Tabber()
        self.wipers = WipeList()

        if self.p_cck.get_last_status()[0] == 'C':
            self.purge_jrnl(True, True)
            self.p_stt.wrt("Last change log recovered")
        self.init()

    def __del__(self):
        try:
            if hasattr(self, 'js') and self.js and not self.js.closed:
                self.js.close()
        except Exception as e:
            print(f"Error closing journal file in __del__: {e}")

    def init(self):
        rd_pt = -1
        wrt_pt = 24
        bytes_stored = 0
        self.js.seek(0)
        self.js.write(struct.pack('qqq', rd_pt, wrt_pt, bytes_stored))

    def wrt_cg_log_to_jrnl(self, r_cg_log: ChangeLog):
        if r_cg_log.cg_line_ct:
            print("\n\tSaving change log:\n")
            r_cg_log.print()

            self.rd_metadata()

            self.js.seek(self.meta_put)
            self.orig_p_pos = self.js.tell()
            try:
                assert self.orig_p_pos >= self.META_LEN, "Original position is less than META_LEN"
            except AssertionError as e:
                print(f"Error in Journal.__init__: {str(e)}")

            self.ttl_bytes = 0
            cg_bytes = 0
            cg_bytes_pos = self.js.tell()

            self.wrt_cgs_to_jrnl(r_cg_log, cg_bytes, cg_bytes_pos)
            actual_written_pos = self.js.tell()  # Add this line and break here
            self.wrt_cgs_sz_to_jrnl(cg_bytes, cg_bytes_pos)

            current_pos = self.js.tell()
            new_g_pos = self.orig_p_pos  # Where this chunk of data starts
            new_p_pos = current_pos  # Where the NEXT chunk should start

            # Handle journal wraparound
            if new_p_pos >= u32Const.JRNL_SIZE.value - self.META_LEN:
                new_p_pos = self.META_LEN  # Reset to start, after metadata

            bytes_written = current_pos - self.orig_p_pos  # How much we wrote this time

            # Update total metadata
            self.meta_get = new_g_pos
            self.meta_put = new_p_pos
            self.meta_sz += bytes_written

            # Write updated metadata to start of file
            self.wrt_metadata(self.meta_get, self.meta_put, self.meta_sz)

            print(f"DEBUG: Updated metadata - get: {self.meta_get}, put: {self.meta_put}, size: {self.meta_sz}")

            print(f"\tChange log written to journal at time {get_cur_time()}")
            r_cg_log.cg_line_ct = 0
            self.p_stt.wrt("Change log written")

    def purge_jrnl(self, keep_going: bool, had_crash: bool):
        print(f"{self.tabs(1, True)}Purging journal{'(after crash)' if had_crash else ''}")

        if not any(self.blks_in_jrnl) and not had_crash:
            print("\tJournal is empty: nothing to purge\n")
        else:
            j_cg_log = ChangeLog()

            self.rd_last_jrnl(j_cg_log)

            if not j_cg_log.the_log:
                print("\tNo changes found in the journal")
            else:
                ctr = 0
                prev_blk_num = SENTINEL_BNUM
                curr_blk_num = SENTINEL_BNUM
                pg = Page()

                self.rd_and_wrt_back(j_cg_log, self.p_buf, ctr, prev_blk_num, curr_blk_num, pg)

                if curr_blk_num in j_cg_log.the_log and j_cg_log.the_log[curr_blk_num]:
                    cg = j_cg_log.the_log[curr_blk_num][-1]
                    self.r_and_wb_last(cg, self.p_buf, ctr, curr_blk_num, pg)
                else:
                    print(f"Warning: No changes found for block {curr_blk_num}")

                assert ctr == 0

            self.blks_in_jrnl = [False] * bNum_tConst.NUM_DISK_BLOCKS.value
            self.p_cL.the_log.clear()

        self.p_stt.wrt("Purged journal" if keep_going else "Finishing")

    def wrt_cg_to_pg(self, cg: Change, pg: Page):
        cg.arr_next = 0

        lin_num = self.get_next_lin_num(cg)
        while lin_num != 0xFF:
            temp = cg.new_data.popleft()
            start = lin_num * u32Const.BYTES_PER_LINE.value
            end = (lin_num + 1) * u32Const.BYTES_PER_LINE.value
            pg.dat[start:end] = temp
            lin_num = self.get_next_lin_num(cg)

    def is_in_jrnl(self, b_num: bNum_t) -> bool:
        return self.blks_in_jrnl[b_num]

    def do_wipe_routine(self, b_num: bNum_t, p_f_m):
        if self.wipers.is_dirty(b_num) or self.wipers.is_ripe():
            p_f_m.do_store_inodes()
            p_f_m.do_store_free_list()
            print(f"\n{self.tabs(1)}Saving change log and purging journal before adding new block")
            self.wrt_cg_log_to_jrnl(self.p_cL)
            self.purge_jrnl()
            self.wipers.clear_array()

    def wrt_field(self, data: bytes, dat_len: int, do_ct: bool):
        p_pos = self.js.tell()
        buf_sz = u32Const.JRNL_SIZE.value
        end_pt = p_pos + dat_len

        if end_pt > buf_sz:
            over = end_pt - buf_sz
            under = dat_len - over
            self.js.write(data[:under])
            if do_ct:
                self.ttl_bytes += under
            self.js.seek(self.META_LEN)
            self.js.write(data[under:])
            if do_ct:
                self.ttl_bytes += over
        elif end_pt == buf_sz:
            self.js.write(data)
            if do_ct:
                self.ttl_bytes += dat_len
            self.js.seek(self.META_LEN)
        else:
            self.js.write(data)
            if do_ct:
                self.ttl_bytes += dat_len
        self.final_p_pos = self.js.tell()

    def advance_strm(self, length: int):
        new_pos = self.js.tell() + length
        if new_pos >= u32Const.JRNL_SIZE.value:
            new_pos -= u32Const.JRNL_SIZE.value
            new_pos += self.META_LEN
        self.js.seek(new_pos)

    def wrt_cgs_to_jrnl(self, r_cg_log: ChangeLog, cg_bytes: int, cg_bytes_pos: int):
        self.ttl_bytes = 0
        initial_pos = self.js.tell()

        # Write a placeholder for cg_bytes
        self.wrt_field(struct.pack('Q', 0), 8, True)

        # Write the rest of the data
        self.wrt_field(struct.pack('Q', self.START_TAG), 8, True)

        for blk_num, changes in r_cg_log.the_log.items():
            for cg in changes:
                self.wrt_field(struct.pack('I', cg.block_num), 4, True)
                self.blks_in_jrnl[cg.block_num] = True
                self.wrt_field(struct.pack('Q', cg.time_stamp), 8, True)

                for s in cg.selectors:
                    self.wrt_field(s.to_bytearray(), self.sz, True)

                for d in cg.new_data:
                    self.wrt_field(d, u32Const.BYTES_PER_LINE.value, True)

        self.wrt_field(struct.pack('Q', self.END_TAG), 8, True)

        # Calculate final cg_bytes
        cg_bytes = self.ttl_bytes - 24

        # Go back and write the actual cg_bytes
        current_pos = self.js.tell()
        self.js.seek(initial_pos)
        self.wrt_field(struct.pack('Q', cg_bytes), 8, False)
        self.js.seek(current_pos)

        self.final_p_pos = self.js.tell()
        # self.js.seek(0, 2)  # Move to the end of the file

        # Update final_p_pos
        # self.final_p_pos = self.js.tell()

        self.do_test1()

    def wrt_cgs_sz_to_jrnl(self, cg_bytes: int, cg_bytes_pos: int):
        try:
            assert 16 <= self.ttl_bytes, "Total bytes is less than 16"
            cg_bytes = self.ttl_bytes - 16

            self.js.seek(cg_bytes_pos)
            self.wrt_field(struct.pack('Q', cg_bytes), 8, False)

            self.js.seek(self.orig_p_pos + self.ttl_bytes)
            self.final_p_pos = self.js.tell()

            assert self.final_p_pos == self.orig_p_pos + self.ttl_bytes, "Final position mismatch"
        except AssertionError as e:
            print(f"Error in wrt_cgs_sz_to_jrnl: {str(e)}")
            # Add additional error handling or logging as needed

    def do_test1(self):
        try:
            assert 0 < self.ttl_bytes < u32Const.JRNL_SIZE.value - self.META_LEN, f"Total bytes out of expected "
            "range: {self.ttl_bytes}"

            expected_bytes = self.final_p_pos - self.orig_p_pos
            if expected_bytes < 0:  # Handle wraparound
                expected_bytes += u32Const.JRNL_SIZE.value - self.META_LEN

            assert self.ttl_bytes == expected_bytes, (
                f"Journal position mismatch: orig_p_pos={self.orig_p_pos}, "
                f"final_p_pos={self.final_p_pos}, ttl_bytes={self.ttl_bytes}, "
                f"expected_bytes={expected_bytes}"
            )
        except AssertionError as e:
            print(f"Error in do_test1: {str(e)}")
            print(f"Current file position: {self.js.tell()}")

    def rd_metadata(self):
        self.js.seek(0)
        self.meta_get, self.meta_put, self.meta_sz = struct.unpack('qqq', self.js.read(24))

    def wrt_metadata(self, new_g_pos: int, new_p_pos: int, u_ttl_bytes: int):
        self.js.seek(0)
        self.js.write(struct.pack('qqq', new_g_pos, new_p_pos, u_ttl_bytes))

    def rd_and_wrt_back(self, j_cg_log: ChangeLog, p_buf: List, ctr: int, prv_blk_num: bNum_t, cur_blk_num: bNum_t,
                        pg: Page):
        for blk_num, changes in j_cg_log.the_log.items():
            for cg in changes:
                cur_blk_num = cg.block_num
                if cur_blk_num != prv_blk_num:
                    if prv_blk_num != SENTINEL_BNUM:
                        if ctr == self.NUM_PGS_JRNL_BUF:
                            self.empty_purge_jrnl_buf(p_buf, ctr)

                        p_buf[ctr] = (prv_blk_num, pg)
                        ctr += 1

                    pg = Page()
                    self.p_d.get_ds().seek(cur_blk_num * u32Const.BLOCK_BYTES.value)
                    pg.dat = bytearray(self.p_d.get_ds().read(u32Const.BLOCK_BYTES.value))

                prv_blk_num = cur_blk_num

                self.wrt_cg_to_pg(cg, pg)

        return ctr, prv_blk_num, cur_blk_num, pg

    def r_and_wb_last(self, cg: Change, p_buf: List, ctr: int, cur_blk_num: bNum_t, pg: Page):
        if ctr == self.NUM_PGS_JRNL_BUF:
            self.empty_purge_jrnl_buf(p_buf, ctr)

        p_buf[ctr] = (cur_blk_num, pg)
        ctr += 1

        self.wrt_cg_to_pg(cg, pg)

        self.empty_purge_jrnl_buf(p_buf, ctr, True)

    def rd_last_jrnl(self, r_j_cg_log: ChangeLog):
        self.rd_metadata()

        # Add this safeguard
        if self.meta_get < self.META_LEN or self.meta_put < self.META_LEN:
            print(f"Warning: Invalid metadata read. meta_get: {self.meta_get}, meta_put: {self.meta_put}")
            return  # Exit early if metadata looks wrong

        self.js.seek(self.meta_get)
        orig_g_pos = self.js.tell()

        try:
            assert orig_g_pos >= self.META_LEN, "Original get position is less than META_LEN"
        except AssertionError as e:
            print(f"Error in rd_last_jrnl: {str(e)}")
            return

        self.ttl_bytes = 0
        cg_bytes = 0
        ck_start_tag = 0
        ck_end_tag = 0

        self.rd_jrnl(r_j_cg_log, cg_bytes, ck_start_tag, ck_end_tag)

        if ck_start_tag != self.START_TAG:
            print(f"Error in rd_last_jrnl: Start tag mismatch: expected {self.START_TAG}, got {ck_start_tag}")
            return

        if ck_end_tag != self.END_TAG:
            print(f"Error in rd_last_jrnl: End tag mismatch: expected {self.END_TAG}, got {ck_end_tag}")
            return

        if self.ttl_bytes != cg_bytes + 24:
            print(f"Error in rd_last_jrnl: Total bytes mismatch: expected {cg_bytes + 24}, got {self.ttl_bytes}")

    def rd_jrnl(self, r_j_cg_log: ChangeLog, cg_bytes: int, ck_start_tag: int, ck_end_tag: int):
        self.ttl_bytes = 0

        cg_bytes = struct.unpack('Q', self.rd_field(8))[0]
        ck_start_tag = struct.unpack('Q', self.rd_field(8))[0]

        while self.ttl_bytes < cg_bytes + 16:  # Include START_TAG and cg_bytes
            b_num = struct.unpack('I', self.rd_field(4))[0]
            if b_num == 0xFFFFFFFF:  # End of changes
                break

            cg = Change(b_num, False)

            cg.time_stamp = struct.unpack('Q', self.rd_field(8))[0]

            num_data_lines = self.get_num_data_lines(cg)

            for _ in range(num_data_lines):
                a_line = self.rd_field(u32Const.BYTES_PER_LINE.value)
                cg.new_data.append(a_line)

            r_j_cg_log.add_to_log(cg)

        ck_end_tag = struct.unpack('Q', self.rd_field(8))[0]

        try:
            assert cg_bytes + 24 == self.ttl_bytes, f"Total bytes mismatch: expected {cg_bytes + 24}, got {self.ttl_bytes}"
        except AssertionError as e:
            print(f"Error in rd_jrnl: {str(e)}")

    def get_num_data_lines(self, r_cg: Change) -> int:
        num_data_lines = 0
        temp_sel = bytearray(b'\xff' * 8)
        setback = 1
        sz_ul = 8
        sz = 8

        temp_sel = self.rd_field(sz)
        r_cg.selectors.append(temp_sel)
        num_data_lines += sz_ul - 1

        while temp_sel[sz_ul - setback] != 0xFF:
            temp_sel = self.rd_field(sz)
            r_cg.selectors.append(temp_sel)
            num_data_lines += sz_ul

        setback += 1
        while setback <= sz_ul and temp_sel[sz_ul - setback] == 0xFF:
            setback += 1
            num_data_lines -= 1

        return num_data_lines

    def rd_field(self, dat_len: int) -> bytes:
        g_pos = self.js.tell()
        buf_sz = u32Const.JRNL_SIZE.value
        end_pt = g_pos + dat_len

        if end_pt > buf_sz:
            over = end_pt - buf_sz
            under = dat_len - over
            data = self.js.read(under)
            self.ttl_bytes += under
            self.js.seek(self.META_LEN)
            data += self.js.read(over)
            self.ttl_bytes += over
        elif end_pt == buf_sz:
            data = self.js.read(dat_len)
            self.ttl_bytes += dat_len
            self.js.seek(self.META_LEN)
        else:
            data = self.js.read(dat_len)
            self.ttl_bytes += dat_len

        return data

    def empty_purge_jrnl_buf(self, p_pg_pr: List[Tuple[bNum_t, Page]], p_ctr: int, is_end: bool = False) -> bool:
        temp = bytearray(u32Const.BLOCK_BYTES.value)
        self.p_d.do_create_block(temp, u32Const.BLOCK_BYTES.value)

        while p_ctr:
            p_ctr -= 1
            cursor = p_pg_pr[p_ctr]
            self.crc_check_pg(cursor)
            self.p_d.get_ds().seek(cursor[0] * u32Const.BLOCK_BYTES.value)

            if self.wipers.is_dirty(cursor[0]):
                self.p_d.get_ds().write(temp)
                print(f"{self.tabs(3, True)}Overwriting dirty block {cursor[0]}")
            else:
                self.p_d.get_ds().write(cursor[1].dat)
                print(f"{self.tabs(3, True)}Writing page {cursor[0]:3} to disk")

        if not is_end:
            print()

        ok_val = True
        if not self.p_d.get_ds():
            print(f"ERROR: Error writing to {self.p_d.get_d_file_name()}")
            ok_val = False

        return ok_val

    def crc_check_pg(self, p_pr: Tuple[bNum_t, Page]):
        p_uc_dat = bytearray(p_pr[1].dat)

        BoostCRC.wrt_bytes_big_e(0x00000000, p_uc_dat[u32Const.BYTES_PER_PAGE.value - u32Const.CRC_BYTES.value:],
                                 u32Const.CRC_BYTES.value)
        code = BoostCRC.get_code(p_uc_dat, u32Const.BYTES_PER_PAGE.value)
        BoostCRC.wrt_bytes_big_e(code, p_uc_dat[u32Const.BYTES_PER_PAGE.value - u32Const.CRC_BYTES.value:],
                                 u32Const.CRC_BYTES.value)
        code2 = BoostCRC.get_code(p_uc_dat, u32Const.BYTES_PER_PAGE.value)

        try:
            assert code2 == 0, "CRC check failed"
        except AssertionError as e:
            print(f"Error in crc_check_pg: {str(e)}")
            # Add additional error handling or logging as needed

    def get_next_lin_num(self, cg: Change) -> lNum_t:
        try:
            assert cg.selectors, "No selectors available"
        except AssertionError as e:
            print(f"Error in get_next_lin_num: {str(e)}")
            # Add additional error handling or logging as needed

        lin_num = cg.selectors[0][cg.arr_next]
        cg.arr_next += 1

        if cg.arr_next == self.sz:
            cg.selectors.popleft()
            cg.arr_next = 0

        return lin_num


if __name__ == "__main__":
    # Basic test setup
    class MockSimDisk:
        def __init__(self):
            self.ds = open("mock_disk.bin", "w+b")
            self.ds.write(b'\0' * u32Const.JRNL_SIZE.value)

        def get_ds(self):
            return self.ds

        def get_d_file_name(self):
            return "mock_disk.bin"

        def do_create_block(self, temp, size):
            temp[:] = b'\0' * size


    class MockChangeLog:
        def __init__(self):
            self.the_log = {}
            self.cg_line_ct = 0

        def print(self):
            print("Mock ChangeLog print")


    class MockStatus:
        def wrt(self, msg):
            print(f"Status: {msg}")


    class MockCrashChk:
        def get_last_status(self):
            return "Normal"


    # Create mock objects
    sim_disk = MockSimDisk()
    change_log = MockChangeLog()
    status = MockStatus()
    crash_chk = MockCrashChk()

    # Create Journal instance
    journal = Journal("mock_journal.bin", sim_disk, change_log, status, crash_chk)

    # Test wrt_cg_log_to_jrnl
    cg = Change(1, True)
    cg.new_data.append(b'A' * u32Const.BYTES_PER_LINE.value)
    change_log.the_log[1] = [cg]
    change_log.cg_line_ct = 1
    journal.wrt_cg_log_to_jrnl(change_log)

    # Test purge_jrnl
    journal.purge_jrnl(True, False)

    # Test is_in_jrnl
    print(f"Is block 1 in journal? {journal.is_in_jrnl(1)}")


    # Test do_wipe_routine
    class MockFileMan:
        def do_store_inodes(self):
            print("Storing inodes")

        def do_store_free_list(self):
            print("Storing free list")


    mock_file_man = MockFileMan()
    journal.do_wipe_routine(1, mock_file_man)

    # Clean up
    import os

    os.remove("mock_disk.bin")
    os.remove("mock_journal.bin")

    print("Journal tests completed.")