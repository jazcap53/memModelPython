from ajTypes import write_64bit, read_64bit, write_32bit, read_32bit, to_bytes_64bit, from_bytes_64bit
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
    # Note: the values of START_TAG and END_TAG were arbitrarily chosen
    # START_TAG = 0xf19186770cf76d4f  # 17406841880640449871
    # END_TAG = 0x3a5d27655ea00dae  # 4205560943366639022
    START_TAG = 0x4f6df70c778691f1
    END_TAG = 0xae0da05e65275d3a
    # START_TAG = 0x4f6df70c778691f1
    # END_TAG = 0xae0da05e65275d3a
    META_LEN = 24
    NUM_PGS_JRNL_BUF = 16
    CPP_SELECT_T_SZ = 8

    def __init__(self, f_name: str, sim_disk, change_log, status, crash_chk, debug=False):
        self.debug = debug
        self.f_name = f_name
        self.p_d = sim_disk
        self.p_cL = change_log
        self.p_stt = status
        self.p_cck = crash_chk
        self.sz = Journal.CPP_SELECT_T_SZ

        # Initialize the file if it doesn't exist or is too small
        file_existed = os.path.exists(self.f_name)
        self.js = open(self.f_name, "rb+" if file_existed else "wb+")
        self.js.seek(0, 2)  # Go to end of file
        current_size = self.js.tell()
        if current_size < u32Const.JRNL_SIZE.value:
            remaining = u32Const.JRNL_SIZE.value - current_size
            self.js.write(b'\0' * remaining)
        self.js.seek(0)  # Reset to beginning of file
        print(f"Journal file: {'Opened' if file_existed else 'Created'}: {self.f_name}")

        # Verify file size
        self.js.seek(0, 2)  # Go to end
        actual_size = self.js.tell()
        self.js.seek(0)  # Reset to beginning
        if actual_size != u32Const.JRNL_SIZE.value:
            raise RuntimeError(f"Journal file size mismatch. Expected {u32Const.JRNL_SIZE.value}, got {actual_size}")

        self.js.seek(self.META_LEN)

        # Rest of the initialization code
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
        self.js.write(struct.pack('<qqq', rd_pt, wrt_pt, bytes_stored))

    def calculate_cg_bytes(self, r_cg_log: ChangeLog) -> int:
        total_bytes = 0
        for blk_num, changes in r_cg_log.the_log.items():
            for cg in changes:
                # Block number (8 bytes)
                total_bytes += 8

                # Timestamp (8 bytes)
                total_bytes += 8

                # Selectors and actual data
                for selector in cg.selectors:
                    # Selector (8 bytes)
                    total_bytes += 8

                    # Actual data (16 bytes * number of set bits in the selector, excluding MSB)
                    set_bits = bin(selector.value & 0x7FFFFFFFFFFFFFFF).count('1')
                    total_bytes += set_bits * u32Const.BYTES_PER_LINE.value

                    # Break after processing the last selector (MSB set)
                    if selector.is_last_block():
                        break

                # CRC value (4 bytes) and Zero padding (4 bytes)
                total_bytes += 8

        return total_bytes

    def wrt_cg_log_to_jrnl(self, r_cg_log: ChangeLog):
        if not r_cg_log.cg_line_ct:
            return

        print("\n\tSaving change log:\n")
        r_cg_log.print()

        self.rd_metadata()

        self.js.seek(self.meta_put)
        self.orig_p_pos = self.js.tell()
        try:
            assert self.orig_p_pos >= self.META_LEN, "Original position is less than META_LEN"
        except AssertionError as e:
            print(f"Error in wrt_cg_log_to_jrnl: {str(e)}")
            return

        self.ttl_bytes = 0

        # Write START_TAG
        write_64bit(self.js, self.START_TAG)
        self.ttl_bytes += 8

        # Calculate cg_bytes
        cg_bytes = self.calculate_cg_bytes(r_cg_log)

        # Write cg_bytes
        write_64bit(self.js, cg_bytes)
        self.ttl_bytes += 8

        # Call to wrt_cgs_to_jrnl
        self.wrt_cgs_to_jrnl(r_cg_log)

        # Write END_TAG
        current_pos = self.js.tell()
        if current_pos >= u32Const.JRNL_SIZE.value:
            current_pos = self.META_LEN + (current_pos % (u32Const.JRNL_SIZE.value - self.META_LEN))
        self.js.seek(current_pos)
        write_64bit(self.js, self.END_TAG)
        self.ttl_bytes += 8

        # Update metadata
        new_g_pos = self.orig_p_pos
        new_p_pos = current_pos + 8  # Add 8 to account for END_TAG
        if new_p_pos >= u32Const.JRNL_SIZE.value:
            new_p_pos = self.META_LEN + (new_p_pos % (u32Const.JRNL_SIZE.value - self.META_LEN))

        bytes_written = new_p_pos - self.orig_p_pos
        if bytes_written < 0:  # Handle wrap-around
            bytes_written += u32Const.JRNL_SIZE.value - self.META_LEN

        self.meta_get = new_g_pos
        self.meta_put = new_p_pos
        self.meta_sz += bytes_written

        self.wrt_metadata(self.meta_get, self.meta_put, self.meta_sz)

        self.js.flush()
        os.fsync(self.js.fileno())

        print(f"\tChange log written to journal at time {get_cur_time()}")
        r_cg_log.cg_line_ct = 0
        self.p_stt.wrt("Change log written")

    def write_change(self, cg: Change) -> int:
        bytes_written = 0
        bytes_written += self.wrt_field(to_bytes_64bit(cg.block_num), 8, True)
        bytes_written += self.wrt_field(to_bytes_64bit(cg.time_stamp), 8, True)
        for s in cg.selectors:
            bytes_written += self.wrt_field(s.to_bytearray(), self.sz, True)
        for d in cg.new_data:
            bytes_written += self.wrt_field(d if isinstance(d, bytes) else bytes(d), u32Const.BYTES_PER_LINE.value,
                                            True)
        return bytes_written

    def purge_jrnl(self, keep_going: bool, had_crash: bool):
        if self.debug:
            return

        self.reset_file()  # Reset file state before purging

        # self.js.seek(self.META_LEN, 0)

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

    def wrt_field(self, data: bytes, dat_len: int, do_ct: bool) -> int:
        bytes_written = 0
        p_pos = self.js.tell()
        buf_sz = u32Const.JRNL_SIZE.value
        end_pt = p_pos + dat_len

        if end_pt > buf_sz:
            over = end_pt - buf_sz
            under = dat_len - over

            if dat_len == 8:  # 64-bit value
                value = from_bytes_64bit(data)
                write_64bit(self.js, value & ((1 << (under * 8)) - 1))
                bytes_written += under
                if do_ct:
                    self.ttl_bytes += under
                self.js.seek(self.META_LEN)
                write_64bit(self.js, value >> (under * 8))
                bytes_written += over
            elif dat_len == 4:  # 32-bit value
                value = int.from_bytes(data, byteorder='little')
                write_32bit(self.js, value & ((1 << (under * 8)) - 1))
                bytes_written += under
                if do_ct:
                    self.ttl_bytes += under
                self.js.seek(self.META_LEN)
                write_32bit(self.js, value >> (under * 8))
                bytes_written += over
            else:
                self.js.write(data[:under])
                bytes_written += under
                if do_ct:
                    self.ttl_bytes += under
                self.js.seek(self.META_LEN)
                self.js.write(data[under:])
                bytes_written += over

            if do_ct:
                self.ttl_bytes += over
        else:
            if dat_len == 8:
                write_64bit(self.js, from_bytes_64bit(data))
            elif dat_len == 4:
                write_32bit(self.js, int.from_bytes(data, byteorder='little'))
            else:
                self.js.write(data)
            bytes_written = dat_len
            if do_ct:
                self.ttl_bytes += dat_len

        self.final_p_pos = self.js.tell()
        return bytes_written

    def advance_strm(self, length: int):
        new_pos = self.js.tell() + length
        if new_pos >= u32Const.JRNL_SIZE.value:
            new_pos -= u32Const.JRNL_SIZE.value
            new_pos += self.META_LEN
        self.js.seek(new_pos)

    # def wrt_cgs_to_jrnl(self, r_cg_log: ChangeLog):
    #     for blk_num, changes in r_cg_log.the_log.items():
    #         for cg in changes:
    #             current_pos = self.js.tell()
    #             if current_pos >= u32Const.JRNL_SIZE.value:
    #                 current_pos = self.META_LEN + (current_pos % (u32Const.JRNL_SIZE.value - self.META_LEN))
    #                 self.js.seek(current_pos)
    #
    #             if current_pos % 8 != 0:
    #                 # Pad to 8-byte alignment
    #                 padding = 8 - (current_pos % 8)
    #                 self.js.write(b'\0' * padding)
    #                 self.ttl_bytes += padding
    #
    #             write_64bit(self.js, cg.block_num)
    #             self.ttl_bytes += 8
    #             self.blks_in_jrnl[cg.block_num] = True
    #
    #             write_64bit(self.js, cg.time_stamp)
    #             self.ttl_bytes += 8
    #
    #             for selector in cg.selectors:
    #                 write_64bit(self.js, selector.value)
    #                 self.ttl_bytes += 8
    #
    #                 for i in range(63):  # Exclude the MSb
    #                     if selector.is_set(i):
    #                         if cg.new_data:
    #                             data = cg.new_data.popleft()
    #                             self.wrt_field(data if isinstance(data, bytes) else bytes(data),
    #                                            u32Const.BYTES_PER_LINE.value, True)
    #                         else:
    #                             print(f"Warning: No data available for set bit {i} in selector")
    #
    #             # Write placeholder CRC
    #             write_32bit(self.js, 0xCCCCCCCC)
    #             self.ttl_bytes += 4
    #
    #             # Write zero padding
    #             write_32bit(self.js, 0)
    #             self.ttl_bytes += 4
    #
    #     self.js.flush()

    def wrt_cgs_to_jrnl(self, r_cg_log: ChangeLog):
        for blk_num, changes in r_cg_log.the_log.items():
            for cg in changes:
                current_pos = self.js.tell()
                if current_pos >= u32Const.JRNL_SIZE.value:
                    current_pos = self.META_LEN + (current_pos % (u32Const.JRNL_SIZE.value - self.META_LEN))
                    self.js.seek(current_pos)

                write_64bit(self.js, cg.block_num)
                self.ttl_bytes += 8
                self.blks_in_jrnl[cg.block_num] = True

                write_64bit(self.js, cg.time_stamp)
                self.ttl_bytes += 8

                for selector in cg.selectors:
                    self.js.write(selector.to_bytes())
                    self.ttl_bytes += 8

                # Iterate through selector bits and write data only for set bits
                for i in range(63):
                    if selector.is_set(i):
                        self.wrt_field(cg.new_data[i], u32Const.BYTES_PER_LINE.value, True)

                # Write placeholder CRC
                self.js.write(b'\xcc' * 4)
                self.ttl_bytes += 4

                # Write zero padding
                self.js.write(b'\0' * 4)
                self.ttl_bytes += 4

    def wrt_cgs_sz_to_jrnl(self, cg_bytes: int, cg_bytes_pos: int):
        try:
            assert 16 <= self.ttl_bytes, "Total bytes is less than 16"
            cg_bytes = self.ttl_bytes - 16

            self.js.seek(cg_bytes_pos)
            self.wrt_field(struct.pack('<Q', cg_bytes), 8, False)

            self.js.seek(self.orig_p_pos + self.ttl_bytes)
            self.final_p_pos = self.js.tell()

            assert self.final_p_pos == self.orig_p_pos + self.ttl_bytes, "Final position mismatch"
        except AssertionError as e:
            print(f"Error in wrt_cgs_sz_to_jrnl: {str(e)}")
            # Add additional error handling or logging as needed

    def do_test1(self):
        try:
            assert 0 < self.ttl_bytes < u32Const.JRNL_SIZE.value - self.META_LEN, f"Total bytes out of expected range: {self.ttl_bytes}"

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
        self.meta_get, self.meta_put, self.meta_sz = struct.unpack('<qqq', self.js.read(24))

    def wrt_metadata(self, new_g_pos: int, new_p_pos: int, u_ttl_bytes: int):
        self.js.seek(0)
        metadata = struct.pack('<qqq', new_g_pos, new_p_pos, u_ttl_bytes)
        self.js.write(metadata)
        print(f"DEBUG: Writing metadata: g_pos={new_g_pos}, p_pos={new_p_pos}, ttl_bytes={u_ttl_bytes}")

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
        if self.meta_get == -1:
            print("Warning: No metadata available. Journal might be empty.")
            return
        if self.meta_get < self.META_LEN or self.meta_get >= u32Const.JRNL_SIZE.value:
            print(f"Error: Invalid metadata. meta_get={self.meta_get}")
            return

        self.js.seek(self.META_LEN if self.meta_get == -1 else self.meta_get)
        orig_g_pos = self.js.tell()

        try:
            assert orig_g_pos >= self.META_LEN, "Original get position is less than META_LEN"
        except AssertionError as e:
            print(f"Error in rd_last_jrnl: {str(e)}")
            return

        ck_start_tag = from_bytes_64bit(self.rd_field(8))
        cg_bytes = from_bytes_64bit(self.rd_field(8))

        print(f"DEBUG: Read cg_bytes: {cg_bytes}, ck_start_tag: {ck_start_tag:X}")

        if ck_start_tag != self.START_TAG:
            print(f"Error in rd_last_jrnl: Start tag mismatch: expected {self.START_TAG}, got {hex(ck_start_tag)}")
            return

        # Call rd_jrnl without passing cg_bytes
        ck_start_tag, ck_end_tag, ttl_bytes = self.rd_jrnl(r_j_cg_log)

        if ck_end_tag != self.END_TAG:
            print(f"Error in rd_last_jrnl: End tag mismatch: expected {self.END_TAG}, got {hex(ck_end_tag)}")
            return

        print(
            f"DEBUG: Read from journal - START_TAG: {hex(ck_start_tag)}, END_TAG: {hex(ck_end_tag)}, Total Bytes: {ttl_bytes}")

    def rd_jrnl(self, r_j_cg_log: ChangeLog) -> Tuple[int, int, int]:
        self.ttl_bytes = 0

        ck_start_tag = read_64bit(self.js)
        self.ttl_bytes += 8

        if ck_start_tag != self.START_TAG:
            print(f"Invalid journaled data. Start tag: {ck_start_tag:X} (expected: {self.START_TAG:X})")
            return 0, 0, 0

        cg_bytes = read_64bit(self.js)
        self.ttl_bytes += 8

        print(f"DEBUG: Read cg_bytes: {cg_bytes}, ck_start_tag: {ck_start_tag:X}")

        while self.ttl_bytes < cg_bytes + 16:
            current_pos = self.js.tell()
            if current_pos >= u32Const.JRNL_SIZE.value:
                current_pos = self.META_LEN + (current_pos % (u32Const.JRNL_SIZE.value - self.META_LEN))
                self.js.seek(current_pos)

            # Ensure 8-byte alignment
            if current_pos % 8 != 0:
                padding = 8 - (current_pos % 8)
                self.js.read(padding)
                self.ttl_bytes += padding

            b_num = read_64bit(self.js)
            self.ttl_bytes += 8

            cg = Change(b_num, False)
            cg.time_stamp = read_64bit(self.js)
            self.ttl_bytes += 8

            while True:
                selector_data = self.js.read(8)
                self.ttl_bytes += 8
                selector = Select.from_bytes(selector_data)
                cg.selectors.append(selector)

                for i in range(63):
                    if selector.is_set(i):
                        line_data = self.rd_field(u32Const.BYTES_PER_LINE.value)
                        cg.new_data.append(line_data)

                if selector.is_last_block():
                    break

            # Read CRC placeholder and padding
            self.js.read(8)
            self.ttl_bytes += 8

            r_j_cg_log.add_to_log(cg)

        ck_end_tag = read_64bit(self.js)
        self.ttl_bytes += 8

        if ck_end_tag != self.END_TAG:
            print(f"Invalid journaled data. End tag: {ck_end_tag:X} (expected: {self.END_TAG:X})")

        print(f"DEBUG: Read END_TAG: {ck_end_tag:X}")
        print(f"DEBUG: Total bytes read: {self.ttl_bytes}, Expected: {cg_bytes + 24}")

        return ck_start_tag, ck_end_tag, self.ttl_bytes

    def get_num_data_lines(self, r_cg: Change) -> int:
        num_data_lines = 0
        temp_sel = bytearray(b'\xff' * 8)
        setback = 1
        sz_ul = 8

        for selector in r_cg.selectors:
            temp_sel = selector.to_bytes()
            num_data_lines += sz_ul - 1

            if temp_sel[sz_ul - setback] == 0xFF:
                setback += 1
                while setback <= sz_ul and temp_sel[sz_ul - setback] == 0xFF:
                    setback += 1
                    num_data_lines -= 1

        return min(num_data_lines, 63)  # Ensure we don't exceed 63 lines

    def rd_field(self, dat_len: int) -> bytes:
        g_pos = self.js.tell()
        buf_sz = u32Const.JRNL_SIZE.value
        end_pt = g_pos + dat_len

        if end_pt > buf_sz:
            over = end_pt - buf_sz
            under = dat_len - over

            if dat_len == 8:  # 64-bit value
                low_bits = read_64bit(self.js)
                self.ttl_bytes += under
                self.js.seek(self.META_LEN)
                high_bits = read_64bit(self.js)
                value = (high_bits << (under * 8)) | low_bits
                data = to_bytes_64bit(value)
            elif dat_len == 4:  # 32-bit value
                low_bits = read_32bit(self.js)
                self.ttl_bytes += under
                self.js.seek(self.META_LEN)
                high_bits = read_32bit(self.js)
                value = (high_bits << (under * 8)) | low_bits
                data = value.to_bytes(4, byteorder='little')
            else:
                data = self.js.read(under)
                self.ttl_bytes += under
                self.js.seek(self.META_LEN)
                data += self.js.read(over)

            self.ttl_bytes += over
        else:
            if dat_len == 8:
                data = to_bytes_64bit(read_64bit(self.js))
            elif dat_len == 4:
                data = read_32bit(self.js).to_bytes(4, byteorder='little')
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

        BoostCRC.wrt_bytes_little_e(0x00000000, p_uc_dat[u32Const.BYTES_PER_PAGE.value - u32Const.CRC_BYTES.value:],
                                 u32Const.CRC_BYTES.value)
        code = BoostCRC.get_code(p_uc_dat, u32Const.BYTES_PER_PAGE.value)
        BoostCRC.wrt_bytes_little_e(code, p_uc_dat[u32Const.BYTES_PER_PAGE.value - u32Const.CRC_BYTES.value:],
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

    def reset_file(self):
        """Closes and re-opens the journal file, resetting its position."""
        self.js.close()
        self.js = open(self.f_name, "rb+")
        self.js.seek(0)


if __name__ == "__main__":
    # Basic test setup
    DEBUG = False  # Set this to False to run normal operations

    # Print both big-endian and little-endian representations
    print(f"Journal.START_TAG (as in code, big-endian): 0x{Journal.START_TAG:016x}")
    print(f"Journal.START_TAG (little-endian representation): 0x{Journal.START_TAG.to_bytes(8, 'little').hex()}")
    class MockSimDisk:
        def __init__(self):
            self.ds = open("mock_disk.bin", "wb+")
            self.ds.write(b'\0' * u32Const.JRNL_SIZE.value)

        def get_ds(self):
            return self.ds

        def get_d_file_name(self):
            return "mock_disk.bin"

        def do_create_block(self, temp, size):
            temp[:] = b'\0' * size


    class MockChangeLog(ChangeLog):
        def __init__(self):
            super().__init__()
            self.the_log = {}
            self.cg_line_ct = 0

        def print(self):
            print("Mock ChangeLog print")

        def add_to_log(self, cg: Change):
            if cg.block_num not in self.the_log:
                self.the_log[cg.block_num] = []
            self.the_log[cg.block_num].append(cg)
            self.cg_line_ct += len(cg.new_data)


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

    # # Test wrt_cg_log_to_jrnl
    # cg = Change(1, True)
    # selector = Select()
    # selector.set(0)  # Set the first bit
    # cg.selectors = [selector]  # Replace the default selector with our new one
    # cg.new_data.append(b'A' * u32Const.BYTES_PER_LINE.value)
    # change_log.the_log[1] = [cg]
    # change_log.cg_line_ct = 1
    #
    # print("Testing wrt_cg_log_to_jrnl...")
    # journal.wrt_cg_log_to_jrnl(change_log)
    # print("wrt_cg_log_to_jrnl completed successfully!")

    # Test wrt_cg_log_to_jrnl
    cg = Change(1, True)  # Block 1, and it's the last block
    for i in range(4):  # Set 4 lines as dirty
        cg.add_line(i, b'A' * u32Const.BYTES_PER_LINE.value)
    change_log.add_to_log(cg)  # Use the add_to_log method instead of directly modifying the_log

    print("Testing wrt_cg_log_to_jrnl...")
    journal.wrt_cg_log_to_jrnl(change_log)
    print("wrt_cg_log_to_jrnl completed successfully!")

    if not DEBUG:
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

        def output_file_cleanup():
            import os
            os.remove("mock_disk.bin")
            os.remove("mock_journal.bin")

        # output_file_cleanup()  # Comment in/out as needed

    journal.js.close()

    print("Journal tests completed.")