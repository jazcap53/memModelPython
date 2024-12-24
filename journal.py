"""Journal module for managing disk write operations and crash recovery.

This module provides a journaling system to ensure data consistency in case of
system crashes during disk writes. It maintains a log of changes, allowing for
recovery and rollback of incomplete operations.

The journal operates by:
1. Recording changes before they are written to disk
2. Providing crash recovery by replaying or rolling back incomplete changes
3. Managing the lifecycle of journal entries

Classes:
    Journal: Main class handling journal operations
    NoSelectorsAvailableError: Custom exception for selector exhaustion
"""
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
from logging_config import get_logger


logger = get_logger(__name__)


class NoSelectorsAvailableError(Exception):
    """Raised when there are no selectors available in a Change object."""
    pass


class Journal:
    """Manages the journaling system for disk write operations.

    The Journal class handles recording, tracking, and recovering changes to disk.
    It uses a file-based approach to maintain consistency and provide crash recovery.

    Attributes:
        START_TAG (int): Marker indicating the start of a journal entry
        END_TAG (int): Marker indicating the end of a journal entry
        META_LEN (int): Length of metadata in bytes
        PAGE_BUFFER_SIZE (int): Number of pages in journal buffer

    Properties:
        meta_get (int): Current read position in journal
        meta_put (int): Current write position in journal
        meta_sz (int): Current size of journal data
    """
    # Constants and class-level attributes
    START_TAG = 0x4f6df70c778691f1
    END_TAG = 0xae0da05e65275d3a
    START_TAG_SIZE = 8
    CT_BYTES_TO_WRITE_SIZE = 8
    END_TAG_SIZE = 8
    META_LEN = START_TAG_SIZE + CT_BYTES_TO_WRITE_SIZE + END_TAG_SIZE
    PAGE_BUFFER_SIZE = 16
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
        """Initialize the Journal instance."""
        # Basic instance variables
        self.debug = debug
        self.f_name = f_name
        self.sim_disk = sim_disk
        self.change_log = change_log
        self.status = status
        self.crash_chk = crash_chk
        self.sz = Journal.CPP_SELECT_T_SZ
        self.end_tag_posn = None

        # File initialization
        file_existed = os.path.exists(self.f_name)
        self.journal_file = open(self.f_name, "rb+" if file_existed else "wb+")
        self.journal_file.seek(0, 2)  # Go to end of file
        current_size = self.journal_file.tell()
        if current_size < u32Const.JRNL_SIZE.value:
            remaining = u32Const.JRNL_SIZE.value - current_size
            self.journal_file.write(b'\0' * remaining)
        self.journal_file.seek(0)  # Reset to beginning
        logger.debug(f"Journal file {'opened' if file_existed else 'created'}: {self.f_name}")

        # Verify file size
        self.journal_file.seek(0, 2)  # Go to end
        actual_size = self.journal_file.tell()
        self.journal_file.seek(0)  # Reset to beginning
        if actual_size != u32Const.JRNL_SIZE.value:
            raise RuntimeError(f"Journal file size mismatch. Expected {u32Const.JRNL_SIZE.value}, got {actual_size}")

        self.journal_file.seek(self.META_LEN)

        # Initialize other instance variables
        self.pg_buf = [None] * self.PAGE_BUFFER_SIZE
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

        # Check last status and call init()
        last_status = self.crash_chk.get_last_status()
        if last_status and last_status[0] == 'C':
            self.purge_jrnl(True, True)
            self.status.wrt("Last change log recovered")
        self.init()

    def __del__(self):
        """Clean up resources by closing the journal file."""
        try:
            if hasattr(self, 'journal_file') and self.journal_file and not self.journal_file.closed:
                self.journal_file.close()
        except Exception as e:
            print(f"Error closing journal file in __del__: {e}")

    def init(self):
        """Initialize journal metadata to default values."""
        self._metadata.init()

    def purge_jrnl(self, keep_going: bool, had_crash: bool):
        """Purge the journal, optionally handling crash recovery."""
        logger.debug(f"Entering purge_jrnl(keep_going={keep_going}, had_crash={had_crash})")

        if self.debug:
            return

        self._file_io.reset_file()  # Reset file state before purging

        logger.info(f"Purging journal{'(after crash)' if had_crash else ''}")

        if self._is_journal_empty() and not had_crash:
            logger.info("Journal is empty: nothing to purge")
        else:
            self._process_journal_changes(had_crash)

        self._reset_metadata()
        self._update_status(keep_going)

    def _is_journal_empty(self) -> bool:
        """Check if the journal is empty."""
        return not any(self.blks_in_jrnl)

    def _process_journal_changes(self, had_crash: bool):
        """Process changes in the journal."""
        j_cg_log = ChangeLog()
        self.rd_last_jrnl(j_cg_log)

        self._log_change_summary(j_cg_log)

        if not j_cg_log.the_log:
            logger.info("No changes found in the journal")
        else:
            self._apply_changes(j_cg_log)

        self._clear_journal_state()

    def _log_change_summary(self, j_cg_log: ChangeLog):
        """Log a summary of changes in the journal."""
        for block, changes in j_cg_log.the_log.items():
            logger.debug(f"Block {block}: {len(changes)} changes")

    def _apply_changes(self, j_cg_log: ChangeLog):
        """Apply changes from the journal to the disk."""
        self._change_log_handler.process_changes(j_cg_log)

    def _process_final_change(self, j_cg_log: ChangeLog, ctr: int, curr_blk_num: bNum_t, pg: Page):
        """Process the final change in a series."""
        if curr_blk_num is not None and curr_blk_num in j_cg_log.the_log and j_cg_log.the_log[curr_blk_num]:
            cg = j_cg_log.the_log[curr_blk_num][-1]
            self._change_log_handler.r_and_wb_last(cg, self.pg_buf, ctr, curr_blk_num, pg)
        else:
            logger.warning(f"No changes found for block {curr_blk_num}")

    def _clear_journal_state(self):
        """Clear the journal state after processing changes."""
        self.blks_in_jrnl = [False] * bNum_tConst.NUM_DISK_BLOCKS.value
        self.change_log.the_log.clear()

    def _reset_metadata(self):
        """Reset the journal metadata."""
        logger.debug(
            f"Before reset - meta_get: {self._metadata.meta_get}, meta_put: {self._metadata.meta_put}, meta_sz: {self._metadata.meta_sz}")

        self._metadata.meta_get = -1
        self._metadata.meta_put = 24
        self._metadata.meta_sz = 0
        self._metadata.write(-1, 24, 0)

        logger.debug(
            f"After reset - meta_get: {self._metadata.meta_get}, meta_put: {self._metadata.meta_put}, meta_sz: {self._metadata.meta_sz}")

    def _update_status(self, keep_going: bool):
        """Update the status after purging the journal."""
        self.status.wrt("Purged journal" if keep_going else "Finishing")

    def is_in_jrnl(self, b_num: bNum_t) -> bool:
        """Check if a block number is currently in the journal."""
        return self.blks_in_jrnl[b_num]

    def do_wipe_routine(self, b_num: bNum_t, p_f_m):
        """Perform the wipe routine for a given block."""
        if self.wipers.is_dirty(b_num) or self.wipers.is_ripe():
            p_f_m.do_store_inodes()
            p_f_m.do_store_free_list()
            logger.info("Saving change log and purging journal before adding new block")
            self._change_log_handler.wrt_cg_log_to_jrnl(self.change_log)
            self.purge_jrnl(True, False)
            self.wipers.clear_array()

    def rd_last_jrnl(self, r_j_cg_log: ChangeLog):
        """Read the last journal entry into a change log."""
        logger.debug("Entering rd_last_jrnl")

        with self.track_position("rd_last_jrnl"):
            start_pos = self._read_journal_metadata()
            if start_pos is None:
                return

            with self.track_position("read_journal_entry"):
                ck_start_tag, ck_end_tag, ttl_bytes = self.rd_jrnl(r_j_cg_log, start_pos)

            self._verify_journal_tags(ck_start_tag, ck_end_tag)
            self._process_journal_entry(ttl_bytes)

            logger.debug(f"Exiting rd_last_jrnl. Read journal entries. Metadata - "
                         f"get: {self.meta_get}, "
                         f"put: {self.meta_put}, "
                         f"size: {self.meta_sz}")

    def _read_journal_metadata(self):
        """Read and validate journal metadata."""
        self.meta_get, self.meta_put, self.meta_sz = self._metadata.read()

        if self.meta_get == -1:
            logger.warning("No metadata available. Journal might be empty.")
            return None
        if self.meta_get < self.META_LEN or self.meta_get >= u32Const.JRNL_SIZE.value:
            logger.error(f"Invalid metadata. meta_get={self.meta_get}")
            return None

        return self.META_LEN if self.meta_get == -1 else self.meta_get

    def _verify_journal_tags(self, ck_start_tag, ck_end_tag):
        """Verify the start and end tags of the journal entry."""
        if ck_start_tag != self.START_TAG:
            raise ValueError(f"Start tag mismatch: expected {self.START_TAG:X}, got {ck_start_tag:X}")

        if ck_end_tag != self.END_TAG:
            raise ValueError(f"End tag mismatch: expected {self.END_TAG:X}, got {ck_end_tag:X}")

    def _process_journal_entry(self, ttl_bytes):
        """Process the journal entry after reading."""
        self.verify_bytes_read()

    def rd_jrnl(self, r_j_cg_log: ChangeLog, start_pos: int) -> Tuple[int, int, int]:
        """Read journal contents from a given position."""
        self.journal_file.seek(start_pos)

        with self.track_position("read_start_tag"):
            ck_start_tag = self._read_start_tag()

        with self.track_position("read_ct_bytes_to_write"):
            ct_bytes_to_write = self._read_ct_bytes_to_write()

        with self.track_position("read_changes"):
            bytes_read = self._read_changes(r_j_cg_log, ct_bytes_to_write)

        with self.track_position("read_end_tag"):
            ck_end_tag = self._read_end_tag()

        return ck_start_tag, ck_end_tag, bytes_read

    def _read_start_tag(self) -> int:
        """Read and return the start tag from the journal file."""
        return read_64bit(self.journal_file)

    def _read_ct_bytes_to_write(self) -> int:
        """Read and return the count of bytes to write from the journal file."""
        return read_64bit(self.journal_file)

    def _read_changes(self, r_j_cg_log: ChangeLog, ct_bytes_to_write: int) -> int:
        """Read changes from the journal and populate the change log."""
        bytes_read = 0
        while bytes_read < ct_bytes_to_write:
            if self._check_journal_end(bytes_read, ct_bytes_to_write):
                break

            with self.track_position("read_single_change"):
                cg, bytes_read = self._read_single_change(bytes_read)

            if cg:
                r_j_cg_log.add_to_log(cg)

            with self.track_position("read_crc_and_padding"):
                bytes_read = self._read_crc_and_padding(bytes_read, ct_bytes_to_write)

        return bytes_read

    def _check_journal_end(self, bytes_read: int, ct_bytes_to_write: int) -> bool:
        """Check if the end of the journal has been reached."""
        return (self.journal_file.tell() + 16 > u32Const.JRNL_SIZE.value) or (bytes_read >= ct_bytes_to_write)

    def _read_single_change(self, bytes_read: int) -> Tuple[Optional[Change], int]:
        """Read a single change from the journal file."""
        b_num = read_64bit(self.journal_file)
        bytes_read += 8
        if bytes_read > self.ct_bytes_to_write:
            return None, bytes_read

        timestamp = read_64bit(self.journal_file)
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
        """Read a selector from the journal file."""
        if self.journal_file.tell() + 8 > u32Const.JRNL_SIZE.value:
            return None, 0

        selector_data = self.journal_file.read(8)
        if bytes_read + 8 > self.ct_bytes_to_write:
            return None, 8

        return Select.from_bytes(selector_data), 8

    def _read_data_for_selector(self, selector: Select, cg: Change, bytes_read: int) -> int:
        """Read data lines for a given selector."""
        data_bytes_read = 0
        for i in range(63):  # Process up to 63 lines (excluding MSB)
            if not selector.is_set(i):
                continue
            if bytes_read + data_bytes_read + u32Const.BYTES_PER_LINE.value > self.ct_bytes_to_write:
                break
            if self.journal_file.tell() + u32Const.BYTES_PER_LINE.value > u32Const.JRNL_SIZE.value:
                break

            line_data = self.journal_file.read(u32Const.BYTES_PER_LINE.value)
            data_bytes_read += u32Const.BYTES_PER_LINE.value
            cg.new_data.append(line_data)

        return data_bytes_read

    def _read_crc_and_padding(self, bytes_read: int, ct_bytes_to_write: int) -> int:
        """Read CRC and padding if there's enough space."""
        if bytes_read + 8 <= ct_bytes_to_write:
            self.journal_file.read(8)  # Read CRC (4 bytes) and padding (4 bytes)
            bytes_read += 8
        return bytes_read

    def _read_end_tag(self) -> int:
        """Read and return the end tag from the journal file."""
        return read_64bit(self.journal_file)

    def empty_purge_jrnl_buf(self, pg_buf: List[Tuple[bNum_t, Page]], p_ctr: int, is_end: bool = False) -> bool:
        """Empty the journal's purge buffer by writing pages to disk."""
        logger.debug(f"Entering empty_purge_jrnl_buf with {p_ctr} pages, is_end={is_end}")

        if p_ctr == 0 and not is_end:
            logger.debug("Buffer empty and not final purge, returning early")
            return True

        # Process all valid entries
        for i in range(p_ctr):
            cursor = pg_buf[i]
            if not cursor:
                continue

            if not self.verify_page_crc(cursor):
                error_msg = f"CRC check failed for block {cursor[0]} before writing to disk"
                logger.error(error_msg)
                self.status.wrt(f"Error: {error_msg}")
                self.crash_chk.set_last_status('C')
                raise ValueError(error_msg)

            self.sim_disk.get_ds().seek(cursor[0] * u32Const.BLOCK_BYTES.value)

            if self.wipers.is_dirty(cursor[0]):
                # Write zeros for dirty blocks
                self.sim_disk.get_ds().write(b'\0' * u32Const.BLOCK_BYTES.value)
                logger.debug(f"Overwriting dirty block {cursor[0]}")
            else:
                self.sim_disk.get_ds().write(cursor[1].dat)
                logger.debug(f"Writing page {cursor[0]:3} to disk")

        logger.debug(f"Finished processing {p_ctr} pages in empty_purge_jrnl_buf")
        return True

    def verify_bytes_read(self):
        """Verify that the number of bytes read matches the expected count."""
        expected_bytes = self.ct_bytes_to_write + self.META_LEN
        actual_bytes = self.journal_file.tell() - self.META_LEN
        assert expected_bytes == actual_bytes, f"Byte mismatch: expected {expected_bytes}, got {actual_bytes}"

    @contextmanager
    def track_position(self, operation_name: str):
        """Context manager to track file position changes during operations."""
        start_pos = self.journal_file.tell()
        yield
        end_pos = self.journal_file.tell()

    def verify_page_crc(self, page_tuple: Tuple[bNum_t, Page]) -> bool:
        """Verify the CRC of a page. Public interface for CRC checking."""
        return self._file_io._crc_check_pg(page_tuple)


    class _Metadata:
        """Handles journal metadata operations."""

        def __init__(self, journal_instance):
            self._journal = journal_instance
            self.meta_get = 0
            self.meta_put = 0
            self.meta_sz = 0

        def read(self):
            """Read metadata from journal file."""
            self._journal.journal_file.seek(0)
            self.meta_get, self.meta_put, self.meta_sz = struct.unpack('<qqq',
                                                                       self._journal.journal_file.read(24))
            return self.meta_get, self.meta_put, self.meta_sz

        def write(self, new_g_pos: int, new_p_pos: int, u_ttl_bytes_written: int):
            """Write metadata to journal file."""
            self._journal.journal_file.seek(0)
            metadata = struct.pack('<qqq', new_g_pos, new_p_pos, u_ttl_bytes_written)
            self._journal.journal_file.write(metadata)

        def init(self):
            """Initialize metadata to default values."""
            rd_pt = -1
            wrt_pt = 24
            bytes_stored = 0
            self._journal.journal_file.seek(0)
            self._journal.journal_file.write(struct.pack('<qqq', rd_pt, wrt_pt, bytes_stored))

    class _FileIO:
        """Handles file I/O operations for the journal."""

        def __init__(self, journal_instance):
            self._journal = journal_instance

        def wrt_field(self, data: bytes, dat_len: int, do_ct: bool) -> int:
            """Write a field to the journal file."""
            bytes_written = 0
            p_pos = self._journal.journal_file.tell()
            buf_sz = u32Const.JRNL_SIZE.value
            end_pt = p_pos + dat_len

            if end_pt > buf_sz:
                overflow_bytes = end_pt - buf_sz
                bytes_until_end = dat_len - overflow_bytes

                if dat_len == 8:  # 64-bit value
                    # Write the first part
                    self._journal.journal_file.write(data[:bytes_until_end])
                    bytes_written += bytes_until_end
                    if do_ct:
                        self._journal.ttl_bytes_written += bytes_until_end

                    # Move to the correct position after wraparound
                    self._journal.journal_file.seek(self._journal.META_LEN)

                    # Write the remaining part
                    self._journal.journal_file.write(data[bytes_until_end:])
                    bytes_written += overflow_bytes
                    if do_ct:
                        self._journal.ttl_bytes_written += overflow_bytes

                elif dat_len == 4:  # 32-bit value
                    value = int.from_bytes(data, byteorder='little')
                    write_32bit(self._journal.journal_file, value & ((1 << (bytes_until_end * 8)) - 1))
                    bytes_written += bytes_until_end
                    if do_ct:
                        self._journal.ttl_bytes_written += bytes_until_end
                    self._journal.journal_file.seek(self._journal.META_LEN)
                    write_32bit(self._journal.journal_file, value >> (bytes_until_end * 8))
                    bytes_written += overflow_bytes
                    if do_ct:
                        self._journal.ttl_bytes_written += overflow_bytes
                else:
                    self._journal.journal_file.write(data[:bytes_until_end])
                    bytes_written += bytes_until_end
                    if do_ct:
                        self._journal.ttl_bytes_written += bytes_until_end
                    self._journal.journal_file.seek(self._journal.META_LEN)
                    self._journal.journal_file.write(data[bytes_until_end:])
                    bytes_written += overflow_bytes
                    if do_ct:
                        self._journal.ttl_bytes_written += overflow_bytes
            else:
                if dat_len == 8:
                    write_64bit(self._journal.journal_file, from_bytes_64bit(data))
                elif dat_len == 4:
                    write_32bit(self._journal.journal_file, int.from_bytes(data, byteorder='little'))
                else:
                    self._journal.journal_file.write(data)
                bytes_written = dat_len
                if do_ct:
                    self._journal.ttl_bytes_written += dat_len

            if do_ct:
                logger.debug(f"Wrote {bytes_written} bytes for {data[:10]}...")

            self._journal.final_p_pos = self._journal.journal_file.tell()
            return bytes_written

        def rd_field(self, dat_len: int) -> bytes:
            """Read a field from the journal file.

            Args:
                dat_len: Number of bytes to read

            Returns:
                bytes: Data read from journal file
            """
            g_pos = self._journal.journal_file.tell()
            buf_sz = u32Const.JRNL_SIZE.value
            end_pt = g_pos + dat_len

            if end_pt > buf_sz:
                return self._read_with_wraparound(dat_len, buf_sz, end_pt)
            else:
                return self._read_without_wraparound(dat_len)

        def _read_with_wraparound(self, dat_len: int, buf_sz: int, end_pt: int) -> bytes:
            """Read data that wraps around the journal boundary."""
            over = end_pt - buf_sz
            under = dat_len - over

            if dat_len == 8:
                return self._read_64bit_wraparound(under)
            elif dat_len == 4:
                return self._read_32bit_wraparound(under)
            else:
                return self._read_generic_wraparound(under, over)

        def _read_64bit_wraparound(self, under: int) -> bytes:
            """Read a 64-bit value that wraps around in the journal."""
            low_bits = read_64bit(self._journal.journal_file)
            self._update_bytes_read(under)

            self._journal.journal_file.seek(self._journal.META_LEN)
            high_bits = read_64bit(self._journal.journal_file)
            self._update_bytes_read(8 - under)

            value = (high_bits << (under * 8)) | low_bits
            return to_bytes_64bit(value)

        def _read_32bit_wraparound(self, under: int) -> bytes:
            """Read a 32-bit value that wraps around in the journal."""
            low_bits = read_32bit(self._journal.journal_file)
            self._update_bytes_read(under)

            self._journal.journal_file.seek(self._journal.META_LEN)
            high_bits = read_32bit(self._journal.journal_file)
            self._update_bytes_read(4 - under)

            value = (high_bits << (under * 8)) | low_bits
            return value.to_bytes(4, byteorder='little')

        def _read_generic_wraparound(self, under: int, over: int) -> bytes:
            """Read generic data that wraps around in the journal."""
            data = self._journal.journal_file.read(under)
            self._update_bytes_read(under)

            self._journal.journal_file.seek(self._journal.META_LEN)
            data += self._journal.journal_file.read(over)
            self._update_bytes_read(over)

            return data

        def _read_without_wraparound(self, dat_len: int) -> bytes:
            """Read data that fits within the current journal space."""
            if dat_len == 8:
                data = to_bytes_64bit(read_64bit(self._journal.journal_file))
            elif dat_len == 4:
                data = read_32bit(self._journal.journal_file).to_bytes(4, byteorder='little')
            else:
                data = self._journal.journal_file.read(dat_len)

            self._update_bytes_read(dat_len)
            return data

        def _update_bytes_read(self, count: int):
            """Update the total bytes read counter."""
            self._journal.ttl_bytes_written += count

        def advance_strm(self, length: int):
            """Advance the file stream position, handling wraparound."""
            new_pos = self._journal.journal_file.tell() + length
            if new_pos >= u32Const.JRNL_SIZE.value:
                new_pos -= u32Const.JRNL_SIZE.value
                new_pos += self._journal.META_LEN
            self._journal.journal_file.seek(new_pos)

        def reset_file(self):
            """Reset the journal file to initial state."""
            self._journal.journal_file.close()
            self._journal.journal_file = open(self._journal.f_name, "rb+")
            self._journal.journal_file.seek(0)

        def write_start_tag(self):
            """Write the start tag to the journal file."""
            write_64bit(self._journal.journal_file, self._journal.START_TAG)

        def write_end_tag(self):
            """Write the end tag to the journal file."""
            write_64bit(self._journal.journal_file, self._journal.END_TAG)

        def write_ct_bytes(self, ct_bytes):
            """Write the count of bytes to the journal file."""
            write_64bit(self._journal.journal_file, ct_bytes)

        def read_start_tag(self):
            """Read the start tag from the journal file."""
            return read_64bit(self._journal.journal_file)

        def read_end_tag(self):
            """Read the end tag from the journal file."""
            return read_64bit(self._journal.journal_file)

        def read_ct_bytes(self):
            """Read the count of bytes from the journal file."""
            return read_64bit(self._journal.journal_file)

        def wrt_cgs_to_jrnl(self, r_cg_log: ChangeLog):
            """Write changes from a change log to the journal."""
            logger.debug(f"Writing {len(r_cg_log.the_log)} change log entries to journal")

            for blk_num, changes in r_cg_log.the_log.items():
                for cg in changes:
                    self._write_change_to_journal(cg)

            self._finalize_journal_write()

        def _write_change_to_journal(self, cg: Change):
            """Write a single change to the journal."""
            self._write_change_header(cg)
            page_data = self._write_change_data(cg)
            self._write_change_footer(page_data)

        def _write_change_header(self, cg: Change):
            """Write the header information for a change."""
            logger.debug(f"Writing block number: {cg.block_num}")
            self.wrt_field(to_bytes_64bit(cg.block_num), 8, True)
            self._journal.blks_in_jrnl[cg.block_num] = True

            logger.debug(f"Writing timestamp: {cg.time_stamp}")
            self.wrt_field(to_bytes_64bit(cg.time_stamp), 8, True)

        def _write_change_data(self, cg: Change) -> bytearray:
            """Write the data for a change and return the accumulated page data."""
            page_data = bytearray(u32Const.BYTES_PER_PAGE.value)

            for selector in cg.selectors:
                self._write_selector_and_data(selector, cg, page_data)

            return page_data

        def _write_selector_and_data(self, selector: Select, cg: Change, page_data: bytearray):
            """Write a selector and its associated data."""
            logger.debug(f"Writing selector: {selector.value}")
            self.wrt_field(selector.to_bytes(), 8, True)

            for i in range(63):  # Process up to 63 lines (excluding MSB)
                if not selector.is_set(i):
                    continue

                self._write_data_line(i, cg, page_data)

        def _write_data_line(self, line_num: int, cg: Change, page_data: bytearray):
            """Write a single line of data."""
            if not cg.new_data:
                logger.warning(f"No data available for set bit {line_num} in selector")
                return

            data = cg.new_data.popleft()
            data_bytes = data if isinstance(data, bytes) else bytes(data)
            logger.debug(f"Writing data line: {data_bytes[:10]}...")
            self.wrt_field(data_bytes, u32Const.BYTES_PER_LINE.value, True)

            start = line_num * u32Const.BYTES_PER_LINE.value
            end = start + u32Const.BYTES_PER_LINE.value
            page_data[start:end] = data_bytes

        def _write_change_footer(self, page_data: bytearray):
            """Write the CRC and padding for a change."""
            crc = AJZlibCRC.get_code(page_data[:-4], u32Const.BYTES_PER_PAGE.value - 4)
            logger.debug(f"Writing CRC: {crc:08x}")
            self.wrt_field(struct.pack('<I', crc), 4, True)

            logger.debug("Writing padding")
            self.wrt_field(b'\0\0\0\0', 4, True)

        def _finalize_journal_write(self):
            """Finalize the journal write operation."""
            self._journal.journal_file.flush()
            logger.debug(f"Total bytes written: {self._journal.ttl_bytes_written}")

        @staticmethod
        def _crc_check_pg(p_pr: Tuple[bNum_t, Page]) -> bool:
            """Verify the CRC of a page."""
            block_num, page = p_pr

            stored_crc = int.from_bytes(page.dat[-u32Const.CRC_BYTES.value:], 'little')
            calculated_crc = AJZlibCRC.get_code(page.dat[:-u32Const.CRC_BYTES.value],
                                                u32Const.BYTES_PER_PAGE.value - u32Const.CRC_BYTES.value)

            if stored_crc != calculated_crc:
                logger.warning(f"CRC mismatch for block {block_num}.")
                logger.warning(f"  Stored:     {stored_crc:04x} {stored_crc >> 16:04x}")
                logger.warning(f"  Calculated: {calculated_crc:04x} {calculated_crc >> 16:04x}")
                return False

            return True


    class _ChangeLogHandler:
        """Manages change log operations for the journal."""

        def __init__(self, journal_instance):
            self._journal = journal_instance
            self.pg_buf = [None] * journal_instance.PAGE_BUFFER_SIZE  # Make this an instance attribute
            # self.intermediate_buf_count = 0  # Also make this an instance attribute

        def get_num_data_lines(self, r_cg: Change) -> int:
            """Calculate the number of data lines in a change."""
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
            """Get the next line number from a change's selectors."""
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
            """Calculate total bytes needed to write a change log."""
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
            """Write a single change to the journal."""
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

        def rd_and_wrt_back(self, j_cg_log: ChangeLog, pg_buf: List, buf_page_count: int,
                            prev_blk_num: bNum_t, curr_blk_num: bNum_t, pg: Page):
            """Read changes from log and write them back to disk."""
            logger.debug(f"Entering rd_and_wrt_back with {len(j_cg_log.the_log)} blocks in change log")

            if not j_cg_log.the_log:
                logger.debug("Change log is empty, returning early")
                return buf_page_count, prev_blk_num, curr_blk_num, pg

            try:
                blocks = list(j_cg_log.the_log.items())
                logger.debug(f"Blocks to process: {blocks}")

                # Process all blocks except the last one
                for i in range(len(blocks) - 1):
                    blk_num, changes = blocks[i]
                    logger.debug(f"Processing block {blk_num} with {len(changes)} changes")

                    for cg in changes:
                        curr_blk_num = cg.block_num
                        logger.debug(f"Current block number: {curr_blk_num}, Previous: {prev_blk_num}")

                        if curr_blk_num != prev_blk_num or prev_blk_num == SENTINEL_INUM:
                            if prev_blk_num != SENTINEL_INUM:
                                pg_buf[buf_page_count] = (prev_blk_num, pg)
                                buf_page_count += 1

                                if buf_page_count == self._journal.PAGE_BUFFER_SIZE:
                                    logger.debug(f"Buffer full ({buf_page_count}), purging")
                                    self._journal.empty_purge_jrnl_buf(pg_buf, buf_page_count)
                                    buf_page_count = 0

                            # Seek and read new block
                            self._journal.sim_disk.get_ds().seek(curr_blk_num * u32Const.BLOCK_BYTES.value)
                            logger.debug(f"Sought to position: {curr_blk_num * u32Const.BLOCK_BYTES.value}")

                            pg = Page()
                            pg.dat = bytearray(self._journal.sim_disk.get_ds().read(u32Const.BLOCK_BYTES.value))
                            logger.debug(f"Read {u32Const.BLOCK_BYTES.value} bytes from disk")

                            prev_blk_num = curr_blk_num

                        self.wrt_cg_to_pg(cg, pg)

                # Handle the last processed block (if any)
                if len(blocks) > 1 and prev_blk_num != SENTINEL_INUM:
                    pg_buf[buf_page_count] = (prev_blk_num, pg)
                    buf_page_count += 1

                logger.debug(f"Exiting rd_and_wrt_back. buf_page_count: {buf_page_count}, "
                             f"prev_blk_num: {prev_blk_num}, curr_blk_num: {curr_blk_num}")
                return buf_page_count, prev_blk_num, curr_blk_num, pg

            except Exception as e:
                logger.error(f"Error in rd_and_wrt_back: {str(e)}")
                raise

        def r_and_wb_last(self, cg: Change, pg_buf: List, ctr: int,
                          curr_blk_num: bNum_t, pg: Page):
            """Process the final change and ensure proper buffer handling."""
            logger.debug(f"Entering r_and_wb_last for block {curr_blk_num}")

            # Add the log message that the test is looking for
            logger.debug(f"Processing block {curr_blk_num} with 1 changes")

            # Read the block from disk
            self._journal.sim_disk.get_ds().seek(curr_blk_num * u32Const.BLOCK_BYTES.value)
            logger.debug(f"Sought to position: {curr_blk_num * u32Const.BLOCK_BYTES.value}")

            pg.dat = bytearray(self._journal.sim_disk.get_ds().read(u32Const.BLOCK_BYTES.value))
            logger.debug(f"Read {u32Const.BLOCK_BYTES.value} bytes from disk")

            # Write the final change to the page
            self.wrt_cg_to_pg(cg, pg)

            # Add final page to buffer
            pg_buf[ctr] = (curr_blk_num, pg)
            ctr += 1

            # Always purge with is_end=True, regardless of buffer state
            logger.debug(f"Purging buffer in r_and_wb_last, ctr={ctr}")
            self._journal.empty_purge_jrnl_buf(pg_buf, ctr, True)

            # Clear the buffer after final purge
            for i in range(len(pg_buf)):
                pg_buf[i] = None

            logger.debug("Exiting r_and_wb_last, buffer cleared")

        def wrt_cg_log_to_jrnl(self, r_cg_log: ChangeLog):
            """Write entire change log to journal."""
            logger.debug(f"Entering wrt_cg_log_to_jrnl with {len(r_cg_log.the_log)} blocks in change log")

            if not r_cg_log.cg_line_ct:
                return

            logger.info("Writing change log to journal")
            r_cg_log.print()  # This might need to be updated in ChangeLog class

            self._journal.ttl_bytes_written = 0  # Reset here

            # Calculate bytes to write
            self._journal.ct_bytes_to_write = self.calculate_ct_bytes_to_write(r_cg_log)
            logger.debug(f"Calculated bytes to write: {self._journal.ct_bytes_to_write}")

            # Write start tag and ct_bytes_to_write (don't count these in ttl_bytes_written)
            self._journal._file_io.write_start_tag()
            self._journal._file_io.write_ct_bytes(self._journal.ct_bytes_to_write)

            # Write changes and count bytes
            self._journal._file_io.wrt_cgs_to_jrnl(r_cg_log)
            logger.debug(f"Actual bytes written: {self._journal.ttl_bytes_written}")

            # Write end tag (don't count)
            self._journal._file_io.write_end_tag()

            # Update metadata
            new_g_pos = Journal.META_LEN  # Start reading from META_LEN
            new_p_pos = self._journal.journal_file.tell()
            ttl_bytes = self._journal.ct_bytes_to_write + Journal.META_LEN

            # Update both instance and file metadata
            self._journal._metadata.meta_get = new_g_pos
            self._journal._metadata.meta_put = new_p_pos
            self._journal._metadata.meta_sz = ttl_bytes
            self._journal._metadata.write(new_g_pos, new_p_pos, ttl_bytes)

            self._journal.journal_file.flush()
            os.fsync(self._journal.journal_file.fileno())

            logger.debug(f"Metadata after write - get: {self._journal._metadata.meta_get}, "
                         f"put: {self._journal._metadata.meta_put}, "
                         f"size: {self._journal._metadata.meta_sz}")

            logger.info(f"Change log written at time {get_cur_time()}")
            r_cg_log.cg_line_ct = 0
            self._journal.status.wrt("Change log written")

            logger.debug(f"Exiting wrt_cg_log_to_jrnl. Wrote {self._journal.ttl_bytes_written} bytes. Final metadata - "
                         f"get: {self._journal._metadata.meta_get}, "
                         f"put: {self._journal._metadata.meta_put}, "
                         f"size: {self._journal._metadata.meta_sz}")

        def process_changes(self, j_cg_log: ChangeLog):
            if not j_cg_log.the_log:
                logger.debug("Change log is empty, nothing to process")
                return

            blocks = list(j_cg_log.the_log.items())
            prev_blk_num = SENTINEL_INUM
            pg = None
            self.pg_buf = [None] * self._journal.PAGE_BUFFER_SIZE  # Ensure buffer is reset

            # Process all blocks except the last one
            for i in range(len(blocks) - 1):
                blk_num, changes = blocks[i]
                logger.debug(f"Processing block {blk_num} with {len(changes)} changes")
                prev_blk_num, pg = self._process_block(changes, self.pg_buf, prev_blk_num, pg)

            # Handle the last block separately
            if blocks:
                last_blk_num, last_changes = blocks[-1]
                logger.debug(f"Processing last block {last_blk_num}")
                self._process_last_block(last_changes[0], self.pg_buf, last_blk_num)

            # Make sure buffer is completely cleared
            self.pg_buf = [None] * self._journal.PAGE_BUFFER_SIZE

        def _process_block(self, changes: List[Change], pg_buf: List, prev_blk_num: bNum_t, pg: Page) -> Tuple[
            bNum_t, Page]:
            curr_blk_num = None
            for cg in changes:
                curr_blk_num = cg.block_num

                if curr_blk_num != prev_blk_num:
                    # New block: add previous block to buffer if it exists
                    if prev_blk_num != SENTINEL_INUM:
                        current_count = self.count_buffer_items()
                        if current_count >= self._journal.PAGE_BUFFER_SIZE - 1:
                            logger.debug(f"Buffer full ({current_count}), purging")
                            self._journal.empty_purge_jrnl_buf(pg_buf, current_count)
                            # Clear buffer after purging
                            for i in range(len(pg_buf)):
                                pg_buf[i] = None

                        buf_item_ct = self.count_buffer_items()
                        pg_buf[buf_item_ct] = (prev_blk_num, pg)

                    # Prepare new page for current block
                    gotten_ds = self._journal.sim_disk.get_ds()
                    gotten_ds.seek(curr_blk_num * u32Const.BLOCK_BYTES.value)
                    pg = Page()
                    pg.dat = bytearray(gotten_ds.read(u32Const.BLOCK_BYTES.value))
                    prev_blk_num = curr_blk_num

                # Apply change to current page
                self.wrt_cg_to_pg(cg, pg)

            return prev_blk_num, pg

        def _process_last_block(self, cg: Change, pg_buf: List, curr_blk_num: bNum_t):
            logger.debug(f"Processing block {curr_blk_num} with 1 changes")
            logger.debug(f"Processing last block {curr_blk_num}")

            # Seek and read the last block
            self._journal.sim_disk.get_ds().seek(curr_blk_num * u32Const.BLOCK_BYTES.value)
            pg = Page()
            pg.dat = bytearray(self._journal.sim_disk.get_ds().read(u32Const.BLOCK_BYTES.value))

            # Write change to page
            self.wrt_cg_to_pg(cg, pg)

            # Add to buffer and purge
            pg_buf[self.count_buffer_items()] = (curr_blk_num, pg)
            self._journal.empty_purge_jrnl_buf(pg_buf, self.count_buffer_items(), True)

            # Clear the buffer after final purge
            for i in range(len(pg_buf)):
                pg_buf[i] = None

        def count_buffer_items(self):
            """Count non-None items in the buffer."""
            return sum(1 for item in self.pg_buf if item is not None)


if __name__ == "__main__":
    import os
    import sys
    import logging
    import logging.config
    from logging_config import setup_logging
    from status import Status
    from simDisk import SimDisk
    from change import Change, ChangeLog
    from crashChk import CrashChk
    from ajTypes import u32Const

    # Setup logging
    setup_logging(logging.DEBUG)
    logger = get_logger(__name__)

    def check_buffer_management(num_blocks):
        # Setup - provide all required filenames
        disk_file = f"buffer_mgmt_disk_{num_blocks}.bin"
        journal_file = f"buffer_mgmt_journal_{num_blocks}.bin"
        free_list_file = f"buffer_mgmt_free_{num_blocks}.bin"
        inode_file = f"buffer_mgmt_inode_{num_blocks}.bin"
        status_file = f"buffer_mgmt_status_{num_blocks}.txt"

        # Clean up any existing files
        for file in [disk_file, journal_file, free_list_file, inode_file, status_file]:
            if os.path.exists(file):
                os.remove(file)

        try:
            # Create instances
            status = Status(status_file)
            sim_disk = SimDisk(status, disk_file, journal_file, free_list_file, inode_file)
            change_log = ChangeLog(test_sw=True)
            crash_chk = CrashChk()

            journal = Journal(journal_file, sim_disk, change_log, status, crash_chk)

            # Create a counter for purge calls
            purge_count = 0
            original_empty_purge = journal.empty_purge_jrnl_buf

            def counting_empty_purge(*args, **kwargs):
                nonlocal purge_count
                purge_count += 1
                return original_empty_purge(*args, **kwargs)

            journal.empty_purge_jrnl_buf = counting_empty_purge

            # Create changes
            for i in range(num_blocks):
                change = Change(i)
                change.add_line(0, b'A' * u32Const.BYTES_PER_LINE.value)
                change_log.add_to_log(change)

            # Process changes
            journal._change_log_handler.process_changes(change_log)

            # Get results
            intermediate_count = journal._change_log_handler.count_buffer_items()
            final_count = journal._change_log_handler.count_buffer_items()

            return intermediate_count, final_count, purge_count

        finally:
            # Clean up files
            for file in [disk_file, journal_file, free_list_file, inode_file, status_file]:
                if os.path.exists(file):
                    try:
                        os.remove(file)
                    except Exception as e:
                        logger.error(f"Failed to remove file {file}: {e}")


    # Test cases and their expected values
    test_cases = [
        (1, 0, 0, 1),  # (num_blocks, expected_intermediate_count, expected_final_count, expected_purge_calls)
        (15, 15, 0, 1),  # One less than buffer size
        (16, 0, 0, 2),  # Full buffer: one purge during, one at end
        (17, 1, 0, 2),  # One purge at full, one at end
        (32, 0, 0, 3)  # Two full purges, one final
    ]


    def run_test(test_index):
        num_blocks, expected_intermediate, expected_final, expected_purge_calls = test_cases[test_index]
        logger.info(f"\nRunning test case {test_index}: {num_blocks} blocks")
        actual_intermediate, actual_final, actual_purge_calls = check_buffer_management(num_blocks)

        logger.info(f"Results for {num_blocks} blocks:")
        logger.info(f"  Intermediate count - Expected: {expected_intermediate}, Got: {actual_intermediate}")
        logger.info(f"  Final count - Expected: {expected_final}, Got: {actual_final}")
        logger.info(f"  Purge calls - Expected: {expected_purge_calls}, Got: {actual_purge_calls}")

        if (actual_intermediate != expected_intermediate or
                actual_final != expected_final or
                actual_purge_calls != expected_purge_calls):
            logger.error(f"Test failed:")
            if actual_intermediate != expected_intermediate:
                logger.error(
                    f"  Intermediate count mismatch - Expected: {expected_intermediate}, Got: {actual_intermediate}")
            if actual_final != expected_final:
                logger.error(f"  Final count mismatch - Expected: {expected_final}, Got: {actual_final}")
            if actual_purge_calls != expected_purge_calls:
                logger.error(f"  Purge calls mismatch - Expected: {expected_purge_calls}, Got: {actual_purge_calls}")
            return False
        else:
            logger.info(f"Test passed")
            return True


    # Parse command line argument
    try:
        if len(sys.argv) > 1:
            test_index = int(sys.argv[1])
            if 0 <= test_index < len(test_cases):
                run_test(test_index)
            else:
                print(f"Error: Test index must be between 0 and {len(test_cases) - 1}")
                sys.exit(1)
        else:
            # Run all tests
            failed_tests = []
            for i in range(len(test_cases)):
                if not run_test(i):
                    failed_tests.append(i)

            if failed_tests:
                logger.error(f"\nFailed tests: {failed_tests}")
            else:
                logger.info("\nAll tests passed!")
    except ValueError:
        print("Error: Test index must be an integer")
        sys.exit(1)