from ajTypes import write_64bit, read_64bit, write_32bit, read_32bit, to_bytes_64bit, from_bytes_64bit
import struct
from typing import List, Dict, Tuple, Optional
from collections import deque
from ajTypes import bNum_t, lNum_t, u32Const, bNum_tConst, SENTINEL_INUM
from ajCrc import BoostCRC
from ajUtils import get_cur_time, Tabber, format_hex_like_hexdump
from wipeList import WipeList
from change import Change, ChangeLog, Select
from myMemory import Page
import os


class NoSelectorsAvailableError(Exception):
    """Raised when there are no selectors available in a Change object."""
    pass


class Journal:
    # Note: the values of START_TAG and END_TAG were arbitrarily chosen
    # START_TAG = 0xf19186770cf76d4f  # 17406841880640449871
    # END_TAG = 0x3a5d27655ea00dae  # 4205560943366639022
    START_TAG = 0x4f6df70c778691f1
    END_TAG = 0xae0da05e65275d3a
    # START_TAG = 0x4f6df70c778691f1
    # END_TAG = 0xae0da05e65275d3a
    START_TAG_SIZE = 8
    CG_BYTES_SIZE = 8
    END_TAG_SIZE = 8
    META_LEN = START_TAG_SIZE + CG_BYTES_SIZE + END_TAG_SIZE
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
        self.end_tag_posn = None  # New data member

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

        # Calculate cg_bytes before writing anything
        cg_bytes = self.calculate_cg_bytes(r_cg_log)

        # Write START_TAG
        write_64bit(self.js, self.START_TAG)

        # Write cg_bytes
        write_64bit(self.js, cg_bytes)

        # Call to wrt_cgs_to_jrnl with byte limit
        self.wrt_cgs_to_jrnl(r_cg_log, cg_bytes)

        # Ensure we've written exactly cg_bytes
        if self.ttl_bytes != cg_bytes:
            print(f"WARNING: Wrote {self.ttl_bytes} bytes, expected {cg_bytes}")
            # Pad or truncate if necessary
            if self.ttl_bytes < cg_bytes:
                self.js.write(b'\0' * (cg_bytes - self.ttl_bytes))
            else:
                self.js.seek(self.orig_p_pos + self.START_TAG_SIZE + self.CG_BYTES_SIZE + cg_bytes)

        # Write END_TAG
        current_pos = self.js.tell()
        if current_pos >= u32Const.JRNL_SIZE.value:
            current_pos = self.META_LEN + (current_pos % (u32Const.JRNL_SIZE.value - self.META_LEN))
        self.js.seek(current_pos)
        write_64bit(self.js, self.END_TAG)
        self.end_tag_posn = self.js.tell()  # Save the position after writing END_TAG

        # Update metadata
        new_g_pos = self.orig_p_pos
        new_p_pos = current_pos + 8  # Add 8 to account for END_TAG
        if new_p_pos >= u32Const.JRNL_SIZE.value:
            new_p_pos = self.META_LEN + (new_p_pos % (u32Const.JRNL_SIZE.value - self.META_LEN))

        self.meta_get = new_g_pos
        self.meta_put = new_p_pos
        self.meta_sz = cg_bytes + self.META_LEN  # META_LEN includes START_TAG, cg_bytes, and END_TAG

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

        print(f"{self.tabs(1, True)}Purging journal{'(after crash)' if had_crash else ''}")

        if not any(self.blks_in_jrnl) and not had_crash:
            print("\tJournal is empty: nothing to purge\n")
        else:
            j_cg_log = ChangeLog()

            self.rd_last_jrnl(j_cg_log)

            print("DEBUG [purge_jrnl]: Content of j_cg_log after rd_last_jrnl:")
            for block, changes in j_cg_log.the_log.items():
                print(f"  Block {block}: {len(changes)} changes")

            if not j_cg_log.the_log:
                print("\tNo changes found in the journal")
            else:
                ctr = 0
                if j_cg_log.the_log:
                    curr_blk_num = next(iter(j_cg_log.the_log))
                    prev_blk_num = None
                else:
                    curr_blk_num = prev_blk_num = None
                pg = Page()

                print("DEBUG [purge_jrnl]: Before calling rd_and_wrt_back")
                print(
                    f"DEBUG [purge_jrnl]: Initial values - ctr: {ctr}, prev_blk_num: {prev_blk_num}, curr_blk_num: {curr_blk_num}")
                ctr, prev_blk_num, curr_blk_num, pg = self.rd_and_wrt_back(j_cg_log, self.p_buf, ctr, prev_blk_num,
                                                                           curr_blk_num, pg)
                print(
                    f"DEBUG [purge_jrnl]: After rd_and_wrt_back: ctr={ctr}, prev_blk_num={prev_blk_num}, curr_blk_num={curr_blk_num}")

                if curr_blk_num in j_cg_log.the_log and j_cg_log.the_log[curr_blk_num]:
                    cg = j_cg_log.the_log[curr_blk_num][-1]
                    print(f"DEBUG [purge_jrnl]: Processing last change for block {curr_blk_num}")
                    print(
                        f"DEBUG [purge_jrnl]: Last change has {len(cg.selectors)} selectors and {len(cg.new_data)} data items")
                    self.r_and_wb_last(cg, self.p_buf, ctr, curr_blk_num, pg)
                else:
                    print(f"Warning: No changes found for block {curr_blk_num}")

                assert ctr == 0

            self.blks_in_jrnl = [False] * bNum_tConst.NUM_DISK_BLOCKS.value
            self.p_cL.the_log.clear()

        self.p_stt.wrt("Purged journal" if keep_going else "Finishing")

    # def wrt_cg_to_pg(self, cg: Change, pg: Page):
    #     print(f"DEBUG [wrt_cg_to_pg]: Starting to write change for block {cg.block_num}")
    #     print(f"DEBUG [wrt_cg_to_pg]: Change has {len(cg.selectors)} selectors and {len(cg.new_data)} data items")
    #
    #     cg.arr_next = 0
    #
    #     try:
    #         while True:
    #             lin_num = self.get_next_lin_num(cg)
    #             if lin_num == 0xFF:
    #                 print("DEBUG [wrt_cg_to_pg]: Reached end of selectors")
    #                 break
    #             if not cg.new_data:
    #                 print("WARNING [wrt_cg_to_pg]: Ran out of data while processing selectors")
    #                 break
    #             temp = cg.new_data.popleft()
    #             start = lin_num * u32Const.BYTES_PER_LINE.value
    #             end = (lin_num + 1) * u32Const.BYTES_PER_LINE.value
    #             pg.dat[start:end] = temp
    #     except NoSelectorsAvailableError:
    #         print("DEBUG [wrt_cg_to_pg]: No more selectors available")
    #
    #     # Calculate and write CRC
    #     crc = BoostCRC.get_code(pg.dat[:-4], u32Const.BYTES_PER_PAGE.value - 4)
    #     BoostCRC.wrt_bytes_little_e(crc, pg.dat[-4:], 4)
    #
    #     print(
    #         f"DEBUG [wrt_cg_to_pg]: Wrote CRC {format_hex_like_hexdump(pg.dat[-4:])} to page for block {cg.block_num}")
    #     print(f"DEBUG [wrt_cg_to_pg]: Wrote CRC {crc:08x} to page for block {cg.block_num}")
    #     print(f"DEBUG [wrt_cg_to_pg]: Last 16 bytes of page: {format_hex_like_hexdump(pg.dat[-16:])}")

    def wrt_cg_to_pg(self, cg: Change, pg: Page):
        print(f"DEBUG [wrt_cg_to_pg]: Starting to write change for block {cg.block_num}")
        print(f"DEBUG [wrt_cg_to_pg]: Change has {len(cg.selectors)} selectors and {len(cg.new_data)} data items")

        cg.arr_next = 0

        try:
            while True:
                lin_num = self.get_next_lin_num(cg)
                if lin_num == 0xFF:
                    print("DEBUG [wrt_cg_to_pg]: Reached end of selectors")
                    break
                if not cg.new_data:
                    print("WARNING [wrt_cg_to_pg]: Ran out of data while processing selectors")
                    break
                temp = cg.new_data.popleft()
                start = lin_num * u32Const.BYTES_PER_LINE.value
                end = (lin_num + 1) * u32Const.BYTES_PER_LINE.value
                pg.dat[start:end] = temp
        except NoSelectorsAvailableError:
            print("DEBUG [wrt_cg_to_pg]: No more selectors available")

        # Calculate and write CRC
        crc = BoostCRC.get_code(pg.dat[:-4], u32Const.BYTES_PER_PAGE.value - 4)
        BoostCRC.wrt_bytes_little_e(crc, pg.dat[-4:], 4)

        print(
            f"DEBUG [wrt_cg_to_pg]: Wrote CRC {format_hex_like_hexdump(pg.dat[-4:])} to page for block {cg.block_num}")
        print(f"DEBUG [wrt_cg_to_pg]: Last 16 bytes of page: {format_hex_like_hexdump(pg.dat[-16:])}")

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

    def wrt_cgs_to_jrnl(self, r_cg_log: ChangeLog, cg_bytes: int):
        bytes_written = 0
        for blk_num, changes in r_cg_log.the_log.items():
            for cg in changes:
                if bytes_written + 24 > cg_bytes:  # 8 for block num, 8 for timestamp, 8 for selector
                    print(f"WARNING: Reached cg_bytes limit ({cg_bytes}) while processing block {blk_num}")
                    return

                current_pos = self.js.tell()
                if current_pos >= u32Const.JRNL_SIZE.value:
                    current_pos = self.META_LEN + (current_pos % (u32Const.JRNL_SIZE.value - self.META_LEN))
                    self.js.seek(current_pos)

                # Write block number
                b_num_bytes = to_bytes_64bit(cg.block_num)
                write_64bit(self.js, cg.block_num)
                print(f"DEBUG [wrt_cgs_to_jrnl]: Writing block number at {current_pos}, appears in hex dump as: {format_hex_like_hexdump(b_num_bytes)}")
                bytes_written += 8
                self.ttl_bytes += 8
                self.blks_in_jrnl[cg.block_num] = True

                # Write timestamp
                ts_bytes = to_bytes_64bit(cg.time_stamp)
                write_64bit(self.js, cg.time_stamp)
                print(f"DEBUG [wrt_cgs_to_jrnl]: Writing timestamp, appears in hex dump as: {format_hex_like_hexdump(ts_bytes)}")
                bytes_written += 8
                self.ttl_bytes += 8

                page_data = bytearray(u32Const.BYTES_PER_PAGE.value)

                for selector in cg.selectors:
                    if bytes_written + 8 > cg_bytes:
                        print(
                            f"WARNING: Reached cg_bytes limit ({cg_bytes}) while processing selector for block {blk_num}")
                        return

                    selector_bytes = selector.to_bytes()
                    self.js.write(selector_bytes)
                    print(f"DEBUG [wrt_cgs_to_jrnl]: Writing selector at {self.js.tell()}, appears in hex dump as: {format_hex_like_hexdump(selector_bytes)}")
                    bytes_written += 8
                    self.ttl_bytes += 8

                    for i in range(63):  # Exclude the MSb
                        if selector.is_set(i):
                            if bytes_written + u32Const.BYTES_PER_LINE.value > cg_bytes:
                                print(
                                    f"WARNING: Reached cg_bytes limit ({cg_bytes}) while processing data for block {blk_num}")
                                return

                            if cg.new_data:
                                data = cg.new_data.popleft()
                                data_bytes = data if isinstance(data, bytes) else bytes(data)
                                print(
                                    f"DEBUG [wrt_cgs_to_jrnl]: Writing data for bit {i}, first 16 bytes appear as: {format_hex_like_hexdump(data_bytes[:16])}")
                                self.wrt_field(data_bytes, u32Const.BYTES_PER_LINE.value, True)
                                start = i * u32Const.BYTES_PER_LINE.value
                                end = start + u32Const.BYTES_PER_LINE.value
                                page_data[start:end] = data_bytes
                                bytes_written += u32Const.BYTES_PER_LINE.value
                            else:
                                print(f"Warning: No data available for set bit {i} in selector")

                # Calculate and write CRC
                if bytes_written + 8 <= cg_bytes:  # 4 for CRC, 4 for padding
                    crc = BoostCRC.get_code(page_data, u32Const.BYTES_PER_PAGE.value)
                    crc_bytes = to_bytes_64bit(crc)[:4]
                    write_32bit(self.js, crc)
                    print(f"DEBUG [wrt_cgs_to_jrnl]: Writing CRC, appears in hex dump as: {format_hex_like_hexdump(crc_bytes)}")
                    self.ttl_bytes += 4
                    bytes_written += 4

                    # Write zero padding
                    write_32bit(self.js, 0)
                    self.ttl_bytes += 4
                    bytes_written += 4

        self.js.flush()

        if bytes_written < cg_bytes:
            padding = cg_bytes - bytes_written
            print(f"DEBUG [wrt_cgs_to_jrnl]: Adding {padding} bytes of padding")
            self.js.write(b'\0' * padding)
            self.ttl_bytes += padding

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
        print(f"DEBUG [rd_and_wrt_back]: Starting with ctr={ctr}, prv_blk_num={prv_blk_num}, cur_blk_num={cur_blk_num}")
        for blk_num, changes in j_cg_log.the_log.items():
            print(f"DEBUG [rd_and_wrt_back]: Processing block {blk_num} with {len(changes)} changes")
            for idx, cg in enumerate(changes):
                print(f"DEBUG [rd_and_wrt_back]: Processing change {idx + 1} for block {blk_num}")
                cur_blk_num = cg.block_num
                if cur_blk_num != prv_blk_num:
                    if prv_blk_num is not None:
                        if ctr == self.NUM_PGS_JRNL_BUF:
                            self.empty_purge_jrnl_buf(p_buf, ctr)

                        p_buf[ctr] = (prv_blk_num, pg)
                        ctr += 1

                    pg = Page()
                    self.p_d.get_ds().seek(cur_blk_num * u32Const.BLOCK_BYTES.value)
                    pg.dat = bytearray(self.p_d.get_ds().read(u32Const.BLOCK_BYTES.value))

                prv_blk_num = cur_blk_num

                print(f"DEBUG [rd_and_wrt_back]: About to write Change for block {cur_blk_num}")
                print(
                    f"DEBUG [rd_and_wrt_back]: Change has {len(cg.selectors)} selectors and {len(cg.new_data)} data items")
                for idx, selector in enumerate(cg.selectors):
                    print(
                        f"DEBUG [rd_and_wrt_back]: Selector {idx}: {format_hex_like_hexdump(to_bytes_64bit(selector.value))}")
                print(f"DEBUG [rd_and_wrt_back]: Last 16 bytes of page: {format_hex_like_hexdump(pg.dat[-16:])}")

                self.wrt_cg_to_pg(cg, pg)

                print(f"DEBUG [rd_and_wrt_back]: After wrt_cg_to_pg for block {cur_blk_num}")
                print(f"DEBUG [rd_and_wrt_back]: Last 16 bytes of page: {format_hex_like_hexdump(pg.dat[-16:])}")

        print(
            f"DEBUG [rd_and_wrt_back]: Finished. Returning ctr={ctr}, prv_blk_num={prv_blk_num}, cur_blk_num={cur_blk_num}")
        return ctr, prv_blk_num, cur_blk_num, pg

    def r_and_wb_last(self, cg: Change, p_buf: List, ctr: int, cur_blk_num: bNum_t, pg: Page):
        if ctr == self.NUM_PGS_JRNL_BUF:
            self.empty_purge_jrnl_buf(p_buf, ctr)

        p_buf[ctr] = (cur_blk_num, pg)
        ctr += 1

        self.wrt_cg_to_pg(cg, pg)

        # Add debug output
        print(f"DEBUG [r_and_wb_last]: Block {cur_blk_num} CRC after writing change:")
        crc = BoostCRC.get_code(pg.dat, u32Const.BYTES_PER_PAGE.value)
        print(f"Calculated CRC: {crc:08x}")
        stored_crc = int.from_bytes(pg.dat[-4:], 'little')
        # print(f"Stored CRC: {stored_crc:08x}")

        print(f"Calculated CRC: {format_hex_like_hexdump(to_bytes_64bit(crc)[:4])}")
        print(f"Stored CRC: {format_hex_like_hexdump(pg.dat[-4:])}")

        self.empty_purge_jrnl_buf(p_buf, ctr, True)

    def rd_last_jrnl(self, r_j_cg_log: ChangeLog):
        self.rd_metadata()
        print(
            f"DEBUG [rd_last_jrnl]: After rd_metadata - meta_get: {self.meta_get}, meta_put: {self.meta_put}, meta_sz: {self.meta_sz}")

        if self.meta_get == -1:
            print("Warning: No metadata available. Journal might be empty.")
            return
        if self.meta_get < self.META_LEN or self.meta_get >= u32Const.JRNL_SIZE.value:
            print(f"Error: Invalid metadata. meta_get={self.meta_get}")
            return

        start_pos = self.META_LEN if self.meta_get == -1 else self.meta_get
        print(f"DEBUG [rd_last_jrnl]: Starting read from position: {start_pos}")

        # Call rd_jrnl with the correct starting position
        ck_start_tag, ck_end_tag, ttl_bytes = self.rd_jrnl(r_j_cg_log, start_pos)

        if ck_start_tag != self.START_TAG:
            print(f"Error in rd_last_jrnl: Start tag mismatch: expected {self.START_TAG:X}, got {ck_start_tag:X}")
            return

        if ck_end_tag != self.END_TAG:
            print(f"Error in rd_last_jrnl: End tag mismatch: expected {self.END_TAG:X}, got {ck_end_tag:X}")
            return

        print(f"DEBUG [rd_last_jrnl]: Read from journal - START_TAG: {format_hex_like_hexdump(to_bytes_64bit(ck_start_tag))}, END_TAG: {format_hex_like_hexdump(to_bytes_64bit(ck_end_tag))}, Total Bytes: {ttl_bytes}")

    def rd_jrnl(self, r_j_cg_log: ChangeLog, start_pos: int) -> Tuple[int, int, int]:
        self.js.seek(start_pos)
        self.ttl_bytes = 0

        ck_start_tag = self._read_start_tag()
        if ck_start_tag != self.START_TAG:
            return 0, 0, 0

        cg_bytes = read_64bit(self.js)
        bytes_read = self._read_changes(r_j_cg_log, cg_bytes)

        ck_end_tag = self._read_end_tag()

        return ck_start_tag, ck_end_tag, bytes_read

    def _read_start_tag(self) -> int:
        return read_64bit(self.js)

    def _read_changes(self, r_j_cg_log: ChangeLog, cg_bytes: int) -> int:
        bytes_read = 0
        while bytes_read < cg_bytes:
            if self._check_journal_end(bytes_read, cg_bytes):
                break

            cg, bytes_read = self._read_single_change(bytes_read, cg_bytes)
            if cg:
                r_j_cg_log.add_to_log(cg)

            if bytes_read + 8 <= cg_bytes:
                self.js.read(8)  # Read CRC (4 bytes) and padding (4 bytes)
                bytes_read += 8

        return bytes_read

    def _check_journal_end(self, bytes_read: int, cg_bytes: int) -> bool:
        return (self.js.tell() + 16 > u32Const.JRNL_SIZE.value) or (bytes_read >= cg_bytes)

    def _read_single_change(self, bytes_read: int, cg_bytes: int) -> Tuple[Optional[Change], int]:
        b_num = read_64bit(self.js)
        bytes_read += 8
        if bytes_read > cg_bytes:
            return None, bytes_read

        timestamp = read_64bit(self.js)
        bytes_read += 8
        if bytes_read > cg_bytes:
            return None, bytes_read

        cg = Change(b_num)
        cg.time_stamp = timestamp

        while bytes_read < cg_bytes:
            selector, selector_bytes_read = self._read_selector(bytes_read, cg_bytes)
            if not selector:
                break
            bytes_read += selector_bytes_read
            cg.selectors.append(selector)

            data_bytes_read = self._read_data_for_selector(selector, cg, bytes_read, cg_bytes)
            bytes_read += data_bytes_read

            if selector.is_last_block():
                break

        return cg, bytes_read

    def _read_selector(self, bytes_read: int, cg_bytes: int) -> Tuple[Optional[Select], int]:
        if self.js.tell() + 8 > u32Const.JRNL_SIZE.value:
            return None, 0

        selector_data = self.js.read(8)
        if bytes_read + 8 > cg_bytes:
            return None, 8

        return Select.from_bytes(selector_data), 8

    def _read_data_for_selector(self, selector: Select, cg: Change, bytes_read: int, cg_bytes: int) -> int:
        data_bytes_read = 0
        for i in range(63):  # Process up to 63 lines (excluding MSB)
            if not selector.is_set(i):
                continue
            if bytes_read + data_bytes_read + u32Const.BYTES_PER_LINE.value > cg_bytes:
                break
            if self.js.tell() + u32Const.BYTES_PER_LINE.value > u32Const.JRNL_SIZE.value:
                break

            line_data = self.js.read(u32Const.BYTES_PER_LINE.value)
            data_bytes_read += u32Const.BYTES_PER_LINE.value
            cg.new_data.append(line_data)

        return data_bytes_read

    # def _read_end_tag(self) -> int:
    #     current_pos = self.js.tell()
    #     if self.end_tag_posn is not None and current_pos != self.end_tag_posn:
    #         print(f"DEBUG [_read_end_tag]: Position mismatch at END_TAG. Expected: {self.end_tag_posn}, Actual: {current_pos}")
    #
    #     ck_end_tag = read_64bit(self.js)
    #     return ck_end_tag

    def _read_end_tag(self) -> int:
        current_pos = self.js.tell()
        if self.end_tag_posn is not None and current_pos != self.end_tag_posn:
            print(
                f"DEBUG [_read_end_tag]: Position mismatch at END_TAG. Expected: {self.end_tag_posn}, Actual: {current_pos}")

        ck_end_tag = read_64bit(self.js)

        after_read_pos = self.js.tell()
        print(f"DEBUG [_read_end_tag]: Position before reading END_TAG: {current_pos}, after reading: {after_read_pos}")

        return ck_end_tag

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

            print(f"DEBUG [empty_purge_jrnl_buf]: Writing block {cursor[0]} to disk")
            print(f"DEBUG [empty_purge_jrnl_buf]: Last 16 bytes of page: {cursor[1].dat[-16:].hex()}")

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

        # Read the stored CRC
        stored_crc = int.from_bytes(p_uc_dat[u32Const.BYTES_PER_PAGE.value - u32Const.CRC_BYTES.value:], 'little')
        print(f"DEBUG [crc_check_pg]: Stored CRC: {stored_crc:08x}")

        # Zero out the CRC bytes for calculation
        BoostCRC.wrt_bytes_little_e(0x00000000, p_uc_dat[u32Const.BYTES_PER_PAGE.value - u32Const.CRC_BYTES.value:],
                                    u32Const.CRC_BYTES.value)

        # Calculate the CRC
        calculated_crc = BoostCRC.get_code(p_uc_dat, u32Const.BYTES_PER_PAGE.value)
        print(f"DEBUG [crc_check_pg]: Calculated CRC: {calculated_crc:08x}")

        if stored_crc != calculated_crc:
            print(
                f"WARNING: CRC mismatch for block {p_pr[0]}. Stored: {stored_crc:08x}, Calculated: {calculated_crc:08x}")
            return False

        # Write the calculated CRC back to the page
        BoostCRC.wrt_bytes_little_e(calculated_crc, p_uc_dat[u32Const.BYTES_PER_PAGE.value - u32Const.CRC_BYTES.value:],
                                    u32Const.CRC_BYTES.value)

        # Recalculate CRC (should be 0)
        final_crc = BoostCRC.get_code(p_uc_dat, u32Const.BYTES_PER_PAGE.value)
        print(f"DEBUG [crc_check_pg]: Final CRC: {final_crc:08x}")

        if final_crc != 0:
            print(f"ERROR: Final CRC check failed for block {p_pr[0]}")
            return False

        return True

    def get_next_lin_num(self, cg: Change) -> lNum_t:
        if not cg.selectors:
            print("DEBUG [get_next_lin_num]: No selectors available")
            return 0xFF  # Return sentinel value immediately if no selectors

        current_selector = cg.selectors[0]
        print(f"DEBUG [get_next_lin_num]: Current selector value: {format_hex_like_hexdump(to_bytes_64bit(current_selector.value))}, arr_next: {cg.arr_next}")

        for i in range(64):
            if current_selector.is_set(i):
                if i == cg.arr_next:
                    cg.arr_next += 1
                    if cg.arr_next == 64:
                        cg.selectors.popleft()
                        cg.arr_next = 0
                    print(f"DEBUG [get_next_lin_num]: Returning line number {i}")
                    return i

        # If we've gone through all bits and found nothing, move to the next selector
        cg.selectors.popleft()
        cg.arr_next = 0
        print("DEBUG [get_next_lin_num]: Moving to next selector")
        return self.get_next_lin_num(cg)  # Recursive call to check next selector

    def reset_file(self):
        """Closes and re-opens the journal file, resetting its position."""
        self.js.close()
        self.js = open(self.f_name, "rb+")
        self.js.seek(0)


if __name__ == "__main__":
    # Basic test setup
    DEBUG = False  # Set this to False to run normal operations

    print(f"Journal.START_TAG: {format_hex_like_hexdump(to_bytes_64bit(Journal.START_TAG))}")
    print(f"Journal.END_TAG: {format_hex_like_hexdump(to_bytes_64bit(Journal.END_TAG))}")

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

    # Test wrt_cg_log_to_jrnl
    cg = Change(1)  # Block 1
    for i in range(4):  # Set 4 lines as dirty
        cg.add_line(i, b'A' * u32Const.BYTES_PER_LINE.value)
    change_log.add_to_log(cg)

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

    journal.js.close()

    print("Journal tests completed.")

    def output_file_cleanup():
        import os
        os.remove("mock_disk.bin")
        os.remove("mock_journal.bin")

    # output_file_cleanup()  # Comment in/out as needed