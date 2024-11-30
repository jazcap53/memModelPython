from ajTypes import write_64bit, read_64bit, write_32bit, read_32bit, to_bytes_64bit, from_bytes_64bit
import struct
from typing import List, Dict, Tuple, Optional
from collections import deque
from ajTypes import bNum_t, lNum_t, u32Const, bNum_tConst, SENTINEL_INUM
from ajCrc import AJZlibCRC
from ajUtils import get_cur_time, Tabber, format_hex_like_hexdump
from wipeList import WipeList
from change import Change, ChangeLog, Select
from myMemory import Page
import os
from contextlib import contextmanager
import logging


# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class NoSelectorsAvailableError(Exception):
    """Raised when there are no selectors available in a Change object."""
    pass


class Journal:
    # Constants and class-level attributes
    START_TAG = 0x4f6df70c778691f1
    END_TAG = 0xae0da05e65275d3a
    START_TAG_SIZE = 8
    CT_BYTES_TO_WRITE_SIZE = 8
    END_TAG_SIZE = 8
    META_LEN = START_TAG_SIZE + CT_BYTES_TO_WRITE_SIZE + END_TAG_SIZE
    NUM_PGS_JRNL_BUF = 16
    CPP_SELECT_T_SZ = 8

    # Properties for backward compatibility
    @property
    def meta_get(self):
        return self._metadata.meta_get

    @meta_get.setter
    def meta_get(self, value):
        self._metadata.meta_get = value

    @property
    def meta_put(self):
        return self._metadata.meta_put

    @meta_put.setter
    def meta_put(self, value):
        self._metadata.meta_put = value

    @property
    def meta_sz(self):
        return self._metadata.meta_sz

    @meta_sz.setter
    def meta_sz(self, value):
        self._metadata.meta_sz = value

    def __init__(self, f_name: str, sim_disk, change_log, status, crash_chk, debug=False):
        # Basic instance variables
        self.debug = debug
        self.f_name = f_name
        self.p_d = sim_disk
        self.p_cL = change_log
        self.p_stt = status
        self.p_cck = crash_chk
        self.sz = Journal.CPP_SELECT_T_SZ
        self.end_tag_posn = None

        # File initialization
        file_existed = os.path.exists(self.f_name)
        self.js = open(self.f_name, "rb+" if file_existed else "wb+")
        self.js.seek(0, 2)  # Go to end of file
        current_size = self.js.tell()
        if current_size < u32Const.JRNL_SIZE.value:
            remaining = u32Const.JRNL_SIZE.value - current_size
            self.js.write(b'\0' * remaining)
        self.js.seek(0)  # Reset to beginning
        logger.debug(f"Journal file {'opened' if file_existed else 'created'}: {self.f_name}")

        # Verify file size
        self.js.seek(0, 2)  # Go to end
        actual_size = self.js.tell()
        self.js.seek(0)  # Reset to beginning
        if actual_size != u32Const.JRNL_SIZE.value:
            raise RuntimeError(f"Journal file size mismatch. Expected {u32Const.JRNL_SIZE.value}, got {actual_size}")

        self.js.seek(self.META_LEN)

        # Initialize other instance variables
        self.p_buf = [None] * self.NUM_PGS_JRNL_BUF
        self.ct_bytes_to_write = 0
        self.ttl_bytes_written = 0
        self.orig_p_pos = 0
        self.final_p_pos = 0
        self.sz = 8  # sizeof(select_t)
        self.sz_ul = 8
        self.blks_in_jrnl = [False] * bNum_tConst.NUM_DISK_BLOCKS.value
        self.last_jrnl_purge_time = 0
        self.tabs = Tabber()
        self.wipers = WipeList()

        # Initialize nested classes
        self._metadata = self._Metadata(self)
        self._file_io = self._FileIO(self)
        self._change_log_handler = self._ChangeLogHandler(self)
        self._crc_handler = self._CRCHandler(self)
        self._purge_handler = self._PurgeHandler(self)

        # Check last status and call init()
        last_status = self.p_cck.get_last_status()
        if last_status and last_status[0] == 'C':
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
        self._metadata.init()

    def calculate_ct_bytes_to_write(self, r_cg_log: ChangeLog) -> int:
        print("DEPRECATED: Use self._change_log_handler.calculate_ct_bytes_to_write() instead")
        return self._change_log_handler.calculate_ct_bytes_to_write(r_cg_log)

    def wrt_cg_log_to_jrnl(self, r_cg_log: ChangeLog):
        logger.debug(f"Entering wrt_cg_log_to_jrnl with {len(r_cg_log.the_log)} blocks in change log")

        if not r_cg_log.cg_line_ct:
            return

        logger.info("Writing change log to journal")
        r_cg_log.print()  # This might need to be updated in ChangeLog class

        self.ttl_bytes_written = 0  # Reset here

        # Use the nested class method
        self.ct_bytes_to_write = self._change_log_handler.calculate_ct_bytes_to_write(r_cg_log)
        logger.debug(f"Calculated bytes to write: {self.ct_bytes_to_write}")

        # Write start tag and ct_bytes_to_write (don't count these in ttl_bytes_written)
        self._file_io.write_start_tag()
        self._file_io.write_ct_bytes(self.ct_bytes_to_write)

        # Write changes and count bytes
        self._file_io.wrt_cgs_to_jrnl(r_cg_log)
        logger.debug(f"Actual bytes written: {self.ttl_bytes_written}")

        # Write end tag (don't count)
        self._file_io.write_end_tag()

        # Update metadata
        new_g_pos = Journal.META_LEN  # Start reading from META_LEN
        new_p_pos = self.js.tell()
        ttl_bytes = self.ct_bytes_to_write + Journal.META_LEN

        # Update both instance and file metadata
        self._metadata.meta_get = new_g_pos
        self._metadata.meta_put = new_p_pos
        self._metadata.meta_sz = ttl_bytes
        self._metadata.write(new_g_pos, new_p_pos, ttl_bytes)

        self.js.flush()
        os.fsync(self.js.fileno())

        logger.debug(f"Metadata after write - get: {self._metadata.meta_get}, "
                     f"put: {self._metadata.meta_put}, "
                     f"size: {self._metadata.meta_sz}")

        logger.info(f"Change log written at time {get_cur_time()}")
        r_cg_log.cg_line_ct = 0
        self.p_stt.wrt("Change log written")

        logger.debug(f"Exiting wrt_cg_log_to_jrnl. Wrote {self.ttl_bytes_written} bytes. Final metadata - "
                     f"get: {self._metadata.meta_get}, "
                     f"put: {self._metadata.meta_put}, "
                     f"size: {self._metadata.meta_sz}")

    def write_change(self, cg: Change) -> int:
        print("DEPRECATED: Use self._change_log_handler.write_change() instead")
        return self._change_log_handler.write_change(cg)

    def purge_jrnl(self, keep_going: bool, had_crash: bool):
        logger.debug(f"Entering purge_jrnl(keep_going={keep_going}, had_crash={had_crash})")

        if self.debug:
            return

        self._file_io.reset_file()  # Reset file state before purging

        logger.info(f"Purging journal{'(after crash)' if had_crash else ''}")

        if not any(self.blks_in_jrnl) and not had_crash:
            logger.info("Journal is empty: nothing to purge")
        else:
            j_cg_log = ChangeLog()

            self.rd_last_jrnl(j_cg_log)

            for block, changes in j_cg_log.the_log.items():
                logger.debug(f"Block {block}: {len(changes)} changes")

            if not j_cg_log.the_log:
                logger.info("No changes found in the journal")
            else:
                ctr = 0
                curr_blk_num = next(iter(j_cg_log.the_log), None) if j_cg_log.the_log else None
                prev_blk_num = None
                pg = Page()

                ctr, prev_blk_num, curr_blk_num, pg = self._change_log_handler.rd_and_wrt_back(j_cg_log,
                                                                                               self.p_buf, ctr,
                                                                                               prev_blk_num,
                                                                                               curr_blk_num, pg)

                if curr_blk_num is not None and curr_blk_num in j_cg_log.the_log and j_cg_log.the_log[curr_blk_num]:
                    cg = j_cg_log.the_log[curr_blk_num][-1]
                    self._change_log_handler.r_and_wb_last(cg, self.p_buf, ctr, curr_blk_num, pg)
                    ctr = 0  # Reset ctr after processing all changes
                else:
                    logger.warning(f"No changes found for block {curr_blk_num}")
                    ctr = 0  # Ensure ctr is reset even if no changes were processed

            self.blks_in_jrnl = [False] * bNum_tConst.NUM_DISK_BLOCKS.value
            self.p_cL.the_log.clear()

        # Reset metadata
        logger.debug(
            f"Before reset - meta_get: {self._metadata.meta_get}, meta_put: {self._metadata.meta_put}, meta_sz: {self._metadata.meta_sz}")

        self._metadata.meta_get = -1
        self._metadata.meta_put = 24
        self._metadata.meta_sz = 0
        self._metadata.write(-1, 24, 0)

        logger.debug(
            f"After reset - meta_get: {self._metadata.meta_get}, meta_put: {self._metadata.meta_put}, meta_sz: {self._metadata.meta_sz}")

        self.p_stt.wrt("Purged journal" if keep_going else "Finishing")

    def wrt_cg_to_pg(self, cg: Change, pg: Page):
        print("DEPRECATED: Use self._change_log_handler.wrt_cg_to_pg() instead")
        return self._change_log_handler.wrt_cg_to_pg(cg, pg)

    def rd_and_wrt_back(self, j_cg_log: ChangeLog, p_buf: List, ctr: int, prv_blk_num: bNum_t, cur_blk_num: bNum_t,
                        pg: Page):
        print("DEPRECATED: Use self._change_log_handler.rd_and_wrt_back() instead")
        return self._change_log_handler.rd_and_wrt_back(j_cg_log, p_buf, ctr, prv_blk_num, cur_blk_num, pg)

    def is_in_jrnl(self, b_num: bNum_t) -> bool:
        return self.blks_in_jrnl[b_num]

    def do_wipe_routine(self, b_num: bNum_t, p_f_m):
        if self.wipers.is_dirty(b_num) or self.wipers.is_ripe():
            p_f_m.do_store_inodes()
            p_f_m.do_store_free_list()
            logger.info("Saving change log and purging journal before adding new block")
            self.wrt_cg_log_to_jrnl(self.p_cL)
            self.purge_jrnl(True, False)
            self.wipers.clear_array()

    def wrt_field(self, data: bytes, dat_len: int, do_ct: bool) -> int:
        start_pos = self._journal.js.tell()
        bytes_written = 0
        p_pos = self._journal.js.tell()
        buf_sz = u32Const.JRNL_SIZE.value
        end_pt = p_pos + dat_len

        if end_pt > buf_sz:
            over = end_pt - buf_sz
            under = dat_len - over

            if dat_len == 8:  # 64-bit value
                self._journal.js.write(data[:under])
                bytes_written += under
                if do_ct:
                    self._journal.ttl_bytes_written += under
                self._journal.js.seek(self._journal.META_LEN)
                self._journal.js.write(data[under:])
                bytes_written += over
                if do_ct:
                    self._journal.ttl_bytes_written += over
            elif dat_len == 4:  # 32-bit value
                value = int.from_bytes(data, byteorder='little')
                write_32bit(self._journal.js, value & ((1 << (under * 8)) - 1))
                bytes_written += under
                if do_ct:
                    self._journal.ttl_bytes_written += under
                self._journal.js.seek(self._journal.META_LEN)
                write_32bit(self._journal.js, value >> (under * 8))
                bytes_written += over
                if do_ct:
                    self._journal.ttl_bytes_written += over
            else:
                self._journal.js.write(data[:under])
                bytes_written += under
                if do_ct:
                    self._journal.ttl_bytes_written += under
                self._journal.js.seek(self._journal.META_LEN)
                self._journal.js.write(data[under:])
                bytes_written += over
                if do_ct:
                    self._journal.ttl_bytes_written += over
        else:
            if dat_len == 8:
                write_64bit(self._journal.js, from_bytes_64bit(data))
            elif dat_len == 4:
                write_32bit(self._journal.js, int.from_bytes(data, byteorder='little'))
            else:
                self._journal.js.write(data)
            bytes_written = dat_len
            if do_ct:
                self._journal.ttl_bytes_written += dat_len

        end_pos = self._journal.js.tell()
        actual_bytes_written = end_pos - start_pos
        if do_ct:
            print(f"DEBUG: Wrote {actual_bytes_written} bytes for {data[:10]}...")

        self._journal.final_p_pos = self._journal.js.tell()
        return bytes_written

    def advance_strm(self, *args, **kwargs):
        import warnings
        warnings.warn(
            "advance_strm is deprecated. Use self._file_io.advance_strm() instead.",
            DeprecationWarning,
            stacklevel=2
        )
        return self._file_io.advance_strm(*args, **kwargs)

    def r_and_wb_last(self, cg: Change, p_buf: List, ctr: int, cur_blk_num: bNum_t, pg: Page):
        print("DEPRECATED: Use self._change_log_handler.r_and_wb_last() instead")
        return self._change_log_handler.r_and_wb_last(cg, p_buf, ctr, cur_blk_num, pg)

    def rd_last_jrnl(self, r_j_cg_log: ChangeLog):
        logger.debug("Entering rd_last_jrnl")

        with self.track_position("rd_last_jrnl"):
            self.meta_get, self.meta_put, self.meta_sz = self._metadata.read()

            if self.meta_get == -1:
                print("Warning: No metadata available. Journal might be empty.")
                return
            if self.meta_get < self.META_LEN or self.meta_get >= u32Const.JRNL_SIZE.value:
                print(f"Error: Invalid metadata. meta_get={self.meta_get}")
                return

            start_pos = self.META_LEN if self.meta_get == -1 else self.meta_get

            with self.track_position("read_journal_entry"):
                ck_start_tag, ck_end_tag, ttl_bytes = self.rd_jrnl(r_j_cg_log, start_pos)

            if ck_start_tag != self.START_TAG:
                raise ValueError(f"Start tag mismatch: expected {self.START_TAG:X}, got {ck_start_tag:X}")

            if ck_end_tag != self.END_TAG:
                raise ValueError(f"End tag mismatch: expected {self.END_TAG:X}, got {ck_end_tag:X}")

            self.verify_bytes_read()

            logger.debug(f"Exiting rd_last_jrnl. Read journal entries. Metadata - "
                         f"get: {self.meta_get}, "
                         f"put: {self.meta_put}, "
                         f"size: {self.meta_sz}")

    def rd_jrnl(self, r_j_cg_log: ChangeLog, start_pos: int) -> Tuple[int, int, int]:
        with self.track_position("read_start_tag"):
            ck_start_tag = self._file_io.read_start_tag()

        with self.track_position("read_ct_bytes_to_write"):
            ct_bytes_to_write = int.from_bytes(self._file_io.rd_field(8), 'little')

        with self.track_position("read_changes"):
            bytes_read = self._read_changes(r_j_cg_log, ct_bytes_to_write)

        with self.track_position("read_end_tag"):
            ck_end_tag = self._file_io.read_end_tag()

        return ck_start_tag, ck_end_tag, bytes_read

    def _read_start_tag(self) -> int:
        return read_64bit(self.js)

    def _read_changes(self, r_j_cg_log: ChangeLog, ct_bytes_to_write: int) -> int:
        bytes_read = 0
        while bytes_read < ct_bytes_to_write:
            if self._check_journal_end(bytes_read, ct_bytes_to_write):
                break

            cg, bytes_read = self._read_single_change(bytes_read)
            if cg:
                r_j_cg_log.add_to_log(cg)

            if bytes_read + 8 <= ct_bytes_to_write:
                self.js.read(8)  # Read CRC (4 bytes) and padding (4 bytes)
                bytes_read += 8

        return bytes_read

    def _check_journal_end(self, bytes_read: int, ct_bytes_to_write: int) -> bool:
        return (self.js.tell() + 16 > u32Const.JRNL_SIZE.value) or (bytes_read >= ct_bytes_to_write)

    def _read_single_change(self, bytes_read: int) -> Tuple[Optional[Change], int]:
        b_num = read_64bit(self.js)
        bytes_read += 8
        if bytes_read > self.ct_bytes_to_write:
            return None, bytes_read

        timestamp = read_64bit(self.js)
        bytes_read += 8
        if bytes_read > self.ct_bytes_to_write:
            return None, bytes_read

        cg = Change(b_num)
        cg.time_stamp = timestamp

        while bytes_read < self.ct_bytes_to_write:
            selector, selector_bytes_read = self._read_selector(bytes_read)
            if not selector:
                break
            bytes_read += selector_bytes_read
            cg.selectors.append(selector)

            data_bytes_read = self._read_data_for_selector(selector, cg, bytes_read)
            bytes_read += data_bytes_read

            if selector.is_last_block():
                break

        return cg, bytes_read

    def _read_selector(self, bytes_read: int) -> Tuple[Optional[Select], int]:
        if self.js.tell() + 8 > u32Const.JRNL_SIZE.value:
            return None, 0

        selector_data = self.js.read(8)
        if bytes_read + 8 > self.ct_bytes_to_write:
            return None, 8

        return Select.from_bytes(selector_data), 8

    def _read_data_for_selector(self, selector: Select, cg: Change, bytes_read: int) -> int:
        data_bytes_read = 0
        for i in range(63):  # Process up to 63 lines (excluding MSB)
            if not selector.is_set(i):
                continue
            if bytes_read + data_bytes_read + u32Const.BYTES_PER_LINE.value > self.ct_bytes_to_write:
                break
            if self.js.tell() + u32Const.BYTES_PER_LINE.value > u32Const.JRNL_SIZE.value:
                break

            line_data = self.js.read(u32Const.BYTES_PER_LINE.value)
            data_bytes_read += u32Const.BYTES_PER_LINE.value
            cg.new_data.append(line_data)

        return data_bytes_read

    def _read_end_tag(self) -> int:
        current_pos = self.js.tell()
        ck_end_tag = read_64bit(self.js)

        if ck_end_tag != self.END_TAG:
            raise ValueError(f"END_TAG mismatch. Expected: {self.END_TAG:X}, Got: {ck_end_tag:X}")

        return ck_end_tag

    def get_num_data_lines(self, r_cg: Change) -> int:
        print("DEPRECATED: Use self._change_log_handler.get_num_data_lines() instead")
        return self._change_log_handler.get_num_data_lines(r_cg)

    def rd_field(self, *args, **kwargs):
        import warnings
        warnings.warn(
            "rd_field is deprecated. Use self._file_io.rd_field() instead.",
            DeprecationWarning,
            stacklevel=2
        )
        return self._file_io.rd_field(*args, **kwargs)

    def empty_purge_jrnl_buf(self, p_pg_pr: List[Tuple[bNum_t, Page]], p_ctr: int, is_end: bool = False) -> bool:
        temp = bytearray(u32Const.BLOCK_BYTES.value)
        self.p_d.do_create_block(temp, u32Const.BLOCK_BYTES.value)

        while p_ctr:
            p_ctr -= 1
            cursor = p_pg_pr[p_ctr]

            # Calculate CRC for the page
            crc = AJZlibCRC.get_code(cursor[1].dat[:-u32Const.CRC_BYTES.value],
                                     u32Const.BYTES_PER_PAGE.value - u32Const.CRC_BYTES.value)

            # Write CRC to the last 4 bytes of the page
            AJZlibCRC.wrt_bytes_little_e(crc, cursor[1].dat[-u32Const.CRC_BYTES.value:], u32Const.CRC_BYTES.value)

            self.p_d.get_ds().seek(cursor[0] * u32Const.BLOCK_BYTES.value)

            if self.wipers.is_dirty(cursor[0]):
                self.p_d.get_ds().write(temp)
                logger.debug(f"Overwriting dirty block {cursor[0]}")
            else:
                self.p_d.get_ds().write(cursor[1].dat)
                logger.debug(f"Writing page {cursor[0]:3} to disk")

        if not is_end:
            logger.debug("Finished writing batch of pages")

        ok_val = True
        if not self.p_d.get_ds():
            logger.error(f"Error writing to {self.p_d.get_d_file_name()}")
            ok_val = False

        return ok_val

    def crc_check_pg(self, p_pr: Tuple[bNum_t, Page]):
        block_num, page = p_pr

        # Read the stored CRC directly from page.dat
        stored_crc = int.from_bytes(page.dat[-u32Const.CRC_BYTES.value:], 'little')

        # Calculate the CRC directly from page.dat
        calculated_crc = AJZlibCRC.get_code(page.dat[:-u32Const.CRC_BYTES.value],
                                            u32Const.BYTES_PER_PAGE.value - u32Const.CRC_BYTES.value)

        if stored_crc != calculated_crc:
            print(f"WARNING [crc_check_pg]: CRC mismatch for block {block_num}.")
            print(f"  Stored:     {stored_crc:04x} {stored_crc >> 16:04x}")
            print(f"  Calculated: {calculated_crc:04x} {calculated_crc >> 16:04x}")
            return False

        return True

    def get_next_lin_num(self, cg: Change) -> lNum_t:
        print("DEPRECATED: Use self._change_log_handler.get_next_lin_num() instead")
        return self._change_log_handler.get_next_lin_num(cg)

    def reset_file(self, *args, **kwargs):
        import warnings
        warnings.warn(
            "reset_file is deprecated. Use self._file_io.reset_file() instead.",
            DeprecationWarning,
            stacklevel=2
        )
        return self._file_io.reset_file(*args, **kwargs)

    def verify_bytes_read(self):
        expected_bytes = self.ct_bytes_to_write + self.META_LEN
        actual_bytes = self.js.tell() - self.META_LEN  # Assuming we start reading after metadata
        assert expected_bytes == actual_bytes, f"Byte mismatch: expected {expected_bytes}, got {actual_bytes}"

    @contextmanager
    def track_position(self, operation_name):
        start_pos = self.js.tell()
        yield
        end_pos = self.js.tell()
        bytes_read = end_pos - start_pos

    # Nested classes
    class _Metadata:
        def __init__(self, journal_instance):
            self._journal = journal_instance
            self.meta_get = 0
            self.meta_put = 0
            self.meta_sz = 0

        def read(self):
            self._journal.js.seek(0)
            self.meta_get, self.meta_put, self.meta_sz = struct.unpack('<qqq',
                self._journal.js.read(24))
            return self.meta_get, self.meta_put, self.meta_sz

        def write(self, new_g_pos: int, new_p_pos: int, u_ttl_bytes_written: int):
            self._journal.js.seek(0)
            metadata = struct.pack('<qqq', new_g_pos, new_p_pos, u_ttl_bytes_written)
            self._journal.js.write(metadata)

        def init(self):
            rd_pt = -1
            wrt_pt = 24
            bytes_stored = 0
            self._journal.js.seek(0)
            self._journal.js.write(struct.pack('<qqq', rd_pt, wrt_pt, bytes_stored))

    class _FileIO:
        def __init__(self, journal_instance):
            self._journal = journal_instance

        def wrt_field(self, data: bytes, dat_len: int, do_ct: bool) -> int:
            bytes_written = 0
            p_pos = self._journal.js.tell()
            buf_sz = u32Const.JRNL_SIZE.value
            end_pt = p_pos + dat_len

            if end_pt > buf_sz:
                overflow_bytes = end_pt - buf_sz
                bytes_until_end = dat_len - overflow_bytes

                if dat_len == 8:  # 64-bit value
                    # Write the first part
                    self._journal.js.write(data[:bytes_until_end])
                    bytes_written += bytes_until_end
                    if do_ct:
                        self._journal.ttl_bytes_written += bytes_until_end

                    # Move to the correct position after wraparound
                    self._journal.js.seek(self._journal.META_LEN)

                    # Write the remaining part
                    self._journal.js.write(data[bytes_until_end:])
                    bytes_written += overflow_bytes
                    if do_ct:
                        self._journal.ttl_bytes_written += overflow_bytes

                elif dat_len == 4:  # 32-bit value
                    value = int.from_bytes(data, byteorder='little')
                    write_32bit(self._journal.js, value & ((1 << (bytes_until_end * 8)) - 1))
                    bytes_written += bytes_until_end
                    if do_ct:
                        self._journal.ttl_bytes_written += bytes_until_end
                    self._journal.js.seek(self._journal.META_LEN)
                    write_32bit(self._journal.js, value >> (bytes_until_end * 8))
                    bytes_written += overflow_bytes
                    if do_ct:
                        self._journal.ttl_bytes_written += overflow_bytes
                else:
                    self._journal.js.write(data[:bytes_until_end])
                    bytes_written += bytes_until_end
                    if do_ct:
                        self._journal.ttl_bytes_written += bytes_until_end
                    self._journal.js.seek(self._journal.META_LEN)
                    self._journal.js.write(data[bytes_until_end:])
                    bytes_written += overflow_bytes
                    if do_ct:
                        self._journal.ttl_bytes_written += overflow_bytes
            else:
                if dat_len == 8:
                    write_64bit(self._journal.js, from_bytes_64bit(data))
                elif dat_len == 4:
                    write_32bit(self._journal.js, int.from_bytes(data, byteorder='little'))
                else:
                    self._journal.js.write(data)
                bytes_written = dat_len
                if do_ct:
                    self._journal.ttl_bytes_written += dat_len

            if do_ct:
                logger.debug(f"Wrote {bytes_written} bytes for {data[:10]}...")

            self._journal.final_p_pos = self._journal.js.tell()
            return bytes_written

        def rd_field(self, dat_len: int) -> bytes:
            g_pos = self._journal.js.tell()
            buf_sz = u32Const.JRNL_SIZE.value
            end_pt = g_pos + dat_len

            if end_pt > buf_sz:
                over = end_pt - buf_sz
                under = dat_len - over

                if dat_len == 8:  # 64-bit value
                    low_bits = read_64bit(self._journal.js)
                    self._journal.ttl_bytes_written += under
                    self._journal.js.seek(self._journal.META_LEN)
                    high_bits = read_64bit(self._journal.js)
                    value = (high_bits << (under * 8)) | low_bits
                    data = to_bytes_64bit(value)
                elif dat_len == 4:  # 32-bit value
                    low_bits = read_32bit(self._journal.js)
                    self._journal.ttl_bytes_written += under
                    self._journal.js.seek(self._journal.META_LEN)
                    high_bits = read_32bit(self._journal.js)
                    value = (high_bits << (under * 8)) | low_bits
                    data = value.to_bytes(4, byteorder='little')
                else:
                    data = self._journal.js.read(under)
                    self._journal.ttl_bytes_written += under
                    self._journal.js.seek(self._journal.META_LEN)
                    data += self._journal.js.read(over)

                self._journal.ttl_bytes_written += over
            else:
                if dat_len == 8:
                    data = to_bytes_64bit(read_64bit(self._journal.js))
                elif dat_len == 4:
                    data = read_32bit(self._journal.js).to_bytes(4, byteorder='little')
                else:
                    data = self._journal.js.read(dat_len)
                self._journal.ttl_bytes_written += dat_len

            return data

        def advance_strm(self, length: int):
            new_pos = self._journal.js.tell() + length
            if new_pos >= u32Const.JRNL_SIZE.value:
                new_pos -= u32Const.JRNL_SIZE.value
                new_pos += self._journal.META_LEN
            self._journal.js.seek(new_pos)

        def reset_file(self):
            self._journal.js.close()
            self._journal.js = open(self._journal.f_name, "rb+")
            self._journal.js.seek(0)

        def write_start_tag(self):
            write_64bit(self._journal.js, self._journal.START_TAG)

        def write_end_tag(self):
            write_64bit(self._journal.js, self._journal.END_TAG)

        def write_ct_bytes(self, ct_bytes):
            write_64bit(self._journal.js, ct_bytes)

        def read_start_tag(self):
            return read_64bit(self._journal.js)

        def read_end_tag(self):
            return read_64bit(self._journal.js)

        def wrt_cgs_to_jrnl(self, r_cg_log: ChangeLog):
            logger.debug(f"Writing {len(r_cg_log.the_log)} change log entries to journal")

            for blk_num, changes in r_cg_log.the_log.items():
                for cg in changes:
                    logger.debug(f"Writing block number: {cg.block_num}")
                    self.wrt_field(to_bytes_64bit(cg.block_num), 8, True)
                    self._journal.blks_in_jrnl[cg.block_num] = True

                    logger.debug(f"Writing timestamp: {cg.time_stamp}")
                    self.wrt_field(to_bytes_64bit(cg.time_stamp), 8, True)

                    page_data = bytearray(u32Const.BYTES_PER_PAGE.value)

                    for selector in cg.selectors:
                        logger.debug(f"Writing selector: {selector.value}")
                        self.wrt_field(selector.to_bytes(), 8, True)

                        for i in range(63):  # Process up to 63 lines (excluding MSB)
                            if not selector.is_set(i):
                                continue

                            if not cg.new_data:
                                logger.warning(f"No data available for set bit {i} in selector")
                                continue

                            data = cg.new_data.popleft()
                            data_bytes = data if isinstance(data, bytes) else bytes(data)
                            logger.debug(f"Writing data line: {data_bytes[:10]}...")
                            self.wrt_field(data_bytes, u32Const.BYTES_PER_LINE.value, True)
                            start = i * u32Const.BYTES_PER_LINE.value
                            end = start + u32Const.BYTES_PER_LINE.value
                            page_data[start:end] = data_bytes

                    # Calculate and write CRC
                    crc = AJZlibCRC.get_code(page_data[:-4], u32Const.BYTES_PER_PAGE.value - 4)
                    logger.debug(f"Writing CRC: {crc:08x}")
                    self.wrt_field(struct.pack('<I', crc), 4, True)
                    logger.debug("Writing padding")
                    self.wrt_field(b'\0\0\0\0', 4, True)

            self._journal.js.flush()
            logger.debug(f"Total bytes written: {self._journal.ttl_bytes_written}")

            logger.debug(f"Exiting _FileIO.wrt_cgs_to_jrnl. Total bytes written: {self._journal.ttl_bytes_written}")

    class _ChangeLogHandler:
        def __init__(self, journal_instance):
            self._journal = journal_instance

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

        def get_next_lin_num(self, cg: Change) -> lNum_t:
            if not cg.selectors:
                return 0xFF  # Return sentinel value immediately if no selectors

            current_selector = cg.selectors[0]

            for i in range(64):
                if current_selector.is_set(i):
                    if i == cg.arr_next:
                        cg.arr_next += 1
                        if cg.arr_next == 64:
                            cg.selectors.popleft()
                            cg.arr_next = 0

                        return i

            # If we've gone through all bits and found nothing, move to the next selector
            cg.selectors.popleft()
            cg.arr_next = 0
            return self.get_next_lin_num(cg)  # Recursive call to check next selector

        def calculate_ct_bytes_to_write(self, r_cg_log: ChangeLog) -> int:
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

        def write_change(self, cg: Change) -> int:
            bytes_written = 0
            bytes_written += self._journal._file_io.wrt_field(to_bytes_64bit(cg.block_num), 8, True)
            bytes_written += self._journal._file_io.wrt_field(to_bytes_64bit(cg.time_stamp), 8, True)
            for s in cg.selectors:
                bytes_written += self._journal._file_io.wrt_field(s.to_bytearray(), self._journal.sz, True)
            for d in cg.new_data:
                bytes_written += self._journal._file_io.wrt_field(
                    d if isinstance(d, bytes) else bytes(d),
                    u32Const.BYTES_PER_LINE.value,
                    True
                )
            return bytes_written

        def wrt_cg_to_pg(self, cg: Change, pg: Page):
            """Write changes to a page."""
            logger.debug("Writing change to page")
            cg.arr_next = 0
            try:
                while True:
                    lin_num = self.get_next_lin_num(cg)
                    if lin_num == 0xFF:
                        break
                    if not cg.new_data:
                        logger.warning("Ran out of data while processing selectors")
                        break
                    temp = cg.new_data.popleft()
                    start = lin_num * u32Const.BYTES_PER_LINE.value
                    end = (lin_num + 1) * u32Const.BYTES_PER_LINE.value
                    logger.debug(f"Writing line {lin_num} to page")
                    pg.dat[start:end] = temp

            except NoSelectorsAvailableError:
                logger.warning("No selectors available")

            # Calculate and write CRC
            crc = AJZlibCRC.get_code(pg.dat[:-4], u32Const.BYTES_PER_PAGE.value - 4)
            pg.dat[-4:] = AJZlibCRC.wrt_bytes_little_e(crc, pg.dat[-4:], 4)
            logger.debug(f"Updated page CRC: {crc:08x}")

        def rd_and_wrt_back(self, j_cg_log: ChangeLog, p_buf: List, ctr: int, prv_blk_num: bNum_t, cur_blk_num: bNum_t,
                            pg: Page):
            if not j_cg_log.the_log:  # Check if the log is empty
                return ctr, prv_blk_num, cur_blk_num, pg

            first_iteration = True
            for blk_num, changes in j_cg_log.the_log.items():
                logger.debug(f"Processing block {blk_num} with {len(changes)} changes")
                for idx, cg in enumerate(changes):
                    cur_blk_num = cg.block_num
                    if cur_blk_num != prv_blk_num or first_iteration:
                        if not first_iteration:  # Don't try to write previous block on first iteration
                            logger.debug(f"New block encountered. ctr before: {ctr}")
                            if ctr == self._journal.NUM_PGS_JRNL_BUF:
                                self._journal.empty_purge_jrnl_buf(p_buf, ctr)
                                ctr = 0  # Reset counter after emptying buffer

                            p_buf[ctr] = (prv_blk_num, pg)
                            ctr += 1
                            logger.debug(f"ctr after incrementing: {ctr}")

                        # Use the provided pg instead of creating a new one
                        self._journal.p_d.get_ds().seek(cur_blk_num * u32Const.BLOCK_BYTES.value)
                        pg.dat = bytearray(self._journal.p_d.get_ds().read(u32Const.BLOCK_BYTES.value))
                        first_iteration = False

                    prv_blk_num = cur_blk_num
                    self.wrt_cg_to_pg(cg, pg)

            # Handle the last block
            if not first_iteration:  # Only if we've processed at least one block
                logger.debug(f"Processing final block. ctr before: {ctr}")
                if ctr == self._journal.NUM_PGS_JRNL_BUF:
                    self._journal.empty_purge_jrnl_buf(p_buf, ctr)
                    ctr = 0
                p_buf[ctr] = (prv_blk_num, pg)
                ctr += 1
                logger.debug(f"ctr after final increment: {ctr}")

            return ctr, prv_blk_num, cur_blk_num, pg

        def r_and_wb_last(self, cg: Change, p_buf: List, ctr: int, cur_blk_num: bNum_t, pg: Page):
            if ctr == self._journal.NUM_PGS_JRNL_BUF:
                self._journal.empty_purge_jrnl_buf(p_buf, ctr)

            p_buf[ctr] = (cur_blk_num, pg)
            ctr += 1

            self.wrt_cg_to_pg(cg, pg)

            # Calculate and store CRC
            crc = AJZlibCRC.get_code(pg.dat, u32Const.BYTES_PER_PAGE.value)
            stored_crc = int.from_bytes(pg.dat[-4:], 'little')

            # Use TRACE level (even more detailed than DEBUG)
            logger.log(5,
                       f"Calculated CRC of entire Page for block {cur_blk_num}: {format_hex_like_hexdump(to_bytes_64bit(crc)[:4])}")
            logger.log(5,
                       f"Stored CRC in Page for block {cur_blk_num}: {format_hex_like_hexdump(to_bytes_64bit(stored_crc)[:4])}")

            self._journal.empty_purge_jrnl_buf(p_buf, ctr, True)


    class _CRCHandler:
        def __init__(self, journal_instance):
            self._journal = journal_instance
            # Stub methods will be added here

    class _PurgeHandler:
        def __init__(self, journal_instance):
            self._journal = journal_instance
            # Stub methods will be added here


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
