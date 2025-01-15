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

    def wrt_cg_log_to_jrnl(self, r_cg_log: ChangeLog):
        """Public method to delegate writing change log to journal to the inner _ChangeLogHandler."""
        self._change_log_handler.wrt_cg_log_to_jrnl(r_cg_log)

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

    def rd_last_jrnl(self, r_j_cg_log: ChangeLog):
        """Read the last journal entry into a change log."""
        logger.debug("Entering rd_last_jrnl")
        current_pos = self.journal_file.tell()
        logger.debug(f"Initial file position: {current_pos}")

        with self.track_position("rd_last_jrnl"):
            start_pos = self._read_journal_metadata()
            logger.debug(f"After reading metadata, position: {self.journal_file.tell()}")
            logger.debug(f"Metadata read: get={self.meta_get}, put={self.meta_put}, sz={self.meta_sz}")

            if start_pos is None:
                logger.warning("No valid start position found")
                return

            with self.track_position("read_journal_entry"):
                logger.debug(f"Starting to read journal entry at position: {start_pos}")
                ck_start_tag, ck_end_tag, ttl_bytes = self.rd_jrnl(r_j_cg_log, start_pos)
                logger.debug(f"After reading journal entry, position: {self.journal_file.tell()}")
                logger.debug(f"Read tags - Start: {ck_start_tag:X}, End: {ck_end_tag:X}")
                logger.debug(f"Total bytes read: {ttl_bytes}")

            try:
                self._verify_journal_tags(ck_start_tag, ck_end_tag)
            except ValueError as e:
                logger.error(f"Tag verification failed: {str(e)}")
                logger.error(f"Current file position: {self.journal_file.tell()}")
                raise

            self._process_journal_entry(ttl_bytes)

            logger.debug(f"Exiting rd_last_jrnl. Read journal entries. Metadata - "
                         f"get: {self.meta_get}, "
                         f"put: {self.meta_put}, "
                         f"size: {self.meta_sz}")

    def rd_jrnl(self, r_j_cg_log: ChangeLog, start_pos: int) -> Tuple[int, int, int]:
        """Read journal contents from a given position."""
        logger.debug(f"Starting journal read from position {start_pos}")

        self.journal_file.seek(start_pos)

        # Read start tag
        with self.track_position("read_start_tag"):
            start_tag_bytes = self._file_io.rd_field(8)
            ck_start_tag = from_bytes_64bit(start_tag_bytes)
            logger.debug(f"Read start tag: {ck_start_tag:X} at position {start_pos}")

        # Read ct_bytes_to_write
        with self.track_position("read_ct_bytes_to_write"):
            ct_bytes_bytes = self._file_io.rd_field(8)
            ct_bytes_to_write = from_bytes_64bit(ct_bytes_bytes)
            self.ct_bytes_to_write = ct_bytes_to_write
            logger.debug(f"Read ct_bytes_to_write: {ct_bytes_to_write}")

        # Read changes
        with self.track_position("read_changes"):
            bytes_read = self._read_changes(r_j_cg_log, ct_bytes_to_write)
            logger.debug(f"Read {bytes_read} bytes of changes")

        # Calculate and seek to end tag position
        end_tag_pos = start_pos + ct_bytes_to_write
        if end_tag_pos >= u32Const.JRNL_SIZE.value:
            end_tag_pos = self._journal.META_LEN + (end_tag_pos - u32Const.JRNL_SIZE.value)
            logger.debug(f"End tag wraps around to position: {end_tag_pos}")
        else:
            logger.debug(f"End tag position without wrap: {end_tag_pos}")

        self.journal_file.seek(end_tag_pos)

        # Read end tag
        with self.track_position("read_end_tag"):
            end_tag_bytes = self._file_io.rd_field(8)
            ck_end_tag = from_bytes_64bit(end_tag_bytes)
            logger.debug(f"Read end tag: {ck_end_tag:X} at position {end_tag_pos}")

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
        position_before = self.journal_file.tell()
        logger.debug(
            f"Starting to read changes. Expecting {ct_bytes_to_write} bytes, starting at position {position_before}")

        while bytes_read < ct_bytes_to_write:
            position_now = self.journal_file.tell()
            if self._check_journal_end(bytes_read, ct_bytes_to_write):
                logger.debug(
                    f"Journal end check stopped read at position {position_now} after reading {bytes_read} bytes")
                break

            with self.track_position("read_single_change"):
                cg, new_bytes_read = self._read_single_change(bytes_read)
                added_bytes = new_bytes_read - bytes_read
                bytes_read = new_bytes_read
                logger.debug(f"Read single change: {added_bytes} bytes")

            if cg:
                r_j_cg_log.add_to_log(cg)
            else:
                logger.debug("No change object returned from _read_single_change")

            with self.track_position("read_crc_and_padding"):
                old_bytes_read = bytes_read
                bytes_read = self._read_crc_and_padding(bytes_read, ct_bytes_to_write)
                logger.debug(f"Read CRC and padding: {bytes_read - old_bytes_read} bytes")

        logger.debug(f"Finished reading changes. Read {bytes_read} of expected {ct_bytes_to_write} bytes")
        return bytes_read

    def _check_journal_end(self, bytes_read: int, ct_bytes_to_write: int) -> bool:
        """Check if we've read all the bytes we need or reached a genuine end."""
        if bytes_read >= ct_bytes_to_write:
            return True

        current_pos = self.journal_file.tell()
        if current_pos + 16 > u32Const.JRNL_SIZE.value:
            # Before returning True, check if we should wrap
            remaining_bytes = ct_bytes_to_write - bytes_read
            if remaining_bytes > 0:
                logger.debug(f"Journal end reached but still need {remaining_bytes} bytes. Wrapping to start.")
                self.journal_file.seek(self.META_LEN)
                return False
        return False

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

    def write_block_to_disk(self, block_num: bNum_t, page: Page):
        """Write a single block to disk.

        This method handles the low-level disk I/O for a single block:
        1. Seeks to the correct disk position
        2. Checks if the block is marked as dirty
        3. Performs the actual write operation

        This method is called by _ChangeLogHandler.write_buffer_to_disk() for each
        block that needs to be written. It handles the physical I/O details that
        the ChangeLogHandler doesn't need to know about.

        Args:
            block_num: The block number to write
            page: The page containing the data to write

        Raises:
            IOError: If the write operation fails

        See Also:
            _ChangeLogHandler.write_buffer_to_disk: Coordinates the overall buffer writing process
        """
        try:
            # Log the disk write operation
            logger.debug(f"Writing block {block_num:3} to disk")

            # Seek to correct position
            self.sim_disk.get_ds().seek(block_num * u32Const.BLOCK_BYTES.value)

            # Check if block is dirty
            if self.wipers.is_dirty(block_num):
                # Write zeros for dirty blocks
                logger.debug(f"  Overwriting dirty block {block_num}")
                self.sim_disk.get_ds().write(b'\0' * u32Const.BLOCK_BYTES.value)
            else:
                # Write actual page data
                self.sim_disk.get_ds().write(page.dat)
        except IOError as e:
            logger.error(f"Failed to write block {block_num} to disk: {e}")
            raise

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

    @classmethod
    def check_buffer_management(cls, num_blocks, test_sw=True):
        """Test buffer management with a specified number of blocks."""
        from simDisk import SimDisk
        from status import Status
        from crashChk import CrashChk

        # Setup - provide all required filenames
        disk_file = f"buffer_mgmt_disk_{num_blocks}.bin"
        journal_file = f"buffer_mgmt_journal_{num_blocks}.bin"
        free_list_file = f"buffer_mgmt_free_{num_blocks}.bin"
        inode_file = f"buffer_mgmt_inode_{num_blocks}.bin"
        status_file = f"buffer_mgmt_status_{num_blocks}.txt"

        # List to track written blocks
        written_blocks = []

        try:
            # Clean up any existing files
            for file in [disk_file, journal_file, free_list_file, inode_file, status_file]:
                if os.path.exists(file):
                    os.remove(file)

            # Create instances
            status = Status(status_file)
            sim_disk = SimDisk(status, disk_file, journal_file, free_list_file, inode_file)
            change_log = ChangeLog(test_sw=test_sw)
            crash_chk = CrashChk()

            journal = cls(journal_file, sim_disk, change_log, status, crash_chk)

            # Create a counter for purge calls
            buffer_write_count = 0
            original_write_buffer = journal._change_log_handler.write_buffer_to_disk

            def counting_write_buffer(*args, **kwargs):
                nonlocal buffer_write_count
                buffer_write_count += 1
                return original_write_buffer(*args, **kwargs)

            journal._change_log_handler.write_buffer_to_disk = counting_write_buffer

            # Mock write_block_to_disk to track written blocks
            original_write_block = journal.write_block_to_disk

            def tracking_write_block(block_num, page):
                written_blocks.append(block_num)
                return original_write_block(block_num, page)

            journal.write_block_to_disk = tracking_write_block

            # Create changes for all blocks
            for i in range(num_blocks):
                change = Change(i)
                change.add_line(0, b'A' * u32Const.BYTES_PER_LINE.value)
                change_log.add_to_log(change)

            # Process all changes
            journal._change_log_handler.process_changes(change_log)

            # Analyze results
            unique_written_blocks = set(written_blocks)
            duplicate_blocks = [b for b in written_blocks if written_blocks.count(b) > 1]

            return {
                'purge_calls': buffer_write_count,
                'written_blocks': written_blocks,
                'unique_written_blocks': unique_written_blocks,
                'duplicate_blocks': duplicate_blocks,
                'all_blocks_written': set(range(num_blocks)).issubset(unique_written_blocks)
            }

        except Exception as e:
            logger.error(f"Error in check_buffer_management: {e}")
            raise

        finally:
            # Clean up files
            for file in [disk_file, journal_file, free_list_file, inode_file, status_file]:
                if os.path.exists(file):
                    try:
                        os.remove(file)
                    except Exception as e:
                        logger.error(f"Failed to remove file {file}: {e}")


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
            logger.debug(f"Reading field of length {dat_len} from position {g_pos}")

            if end_pt > buf_sz:
                logger.debug(f"Field wraps around journal boundary")
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
            os.fsync(self._journal.journal_file.fileno())  # Ensure data is written to disk
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
            self.pg_buf: List[Optional[Tuple[int, Page]]] = [None] * journal_instance.PAGE_BUFFER_SIZE  # Make this an instance attribute
            # self.intermediate_buf_count = 0  # Also make this an instance attribute

        def _handle_block_transition(self, block_num: bNum_t, page: Page):
            """Handle transition between blocks during change processing.

            Args:
                block_num: The current block number.
                page: The current page object.
            """
            if block_num != SENTINEL_INUM:
                self._add_to_buffer(block_num, page)

        def _read_new_block(self, block_num: bNum_t) -> Tuple[bNum_t, Page]:
            """Read a new block from disk.

            Args:
                block_num: The block number to read.

            Returns:
                A tuple containing the block number and the read Page object.
            """
            disk_stream = self._journal.sim_disk.get_ds()
            disk_stream.seek(block_num * u32Const.BLOCK_BYTES.value, 0)
            page = Page()
            page.dat = bytearray(disk_stream.read(u32Const.BLOCK_BYTES.value))
            return block_num, page

        def _apply_change_to_page(self, change: Change, page: Page):
            """Apply a single change to a page.

            Args:
                change: The Change object to apply.
                page: The Page object to modify.
            """
            self.wrt_cg_to_pg(change, page)

        def _add_to_buffer(self, block_num: bNum_t, page: Page):
            """Add a block to the buffer, writing to disk if the buffer is full.

            Args:
                block_num: The block number to add.
                page: The Page object to add.
            """
            current_count = self.count_buffer_items()
            if current_count == self._journal.PAGE_BUFFER_SIZE:
                self.write_buffer_to_disk(False)
                current_count = 0
            self.pg_buf[current_count] = (block_num, page)

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
                                    self.write_buffer_to_disk(False)  # Not the end of processing
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
            logger.debug(f"Processing final block {curr_blk_num}")

            # Read the block from disk
            self._journal.sim_disk.get_ds().seek(curr_blk_num * u32Const.BLOCK_BYTES.value, 0)
            pg.dat = bytearray(self._journal.sim_disk.get_ds().read(u32Const.BLOCK_BYTES.value))

            # Write the final change to the page
            self.wrt_cg_to_pg(cg, pg)

            # Check if buffer is full before adding final page
            if ctr == self._journal.PAGE_BUFFER_SIZE:
                self.write_buffer_to_disk(False)  # Write full buffer
                pg_buf = [None] * self._journal.PAGE_BUFFER_SIZE
                ctr = 0

            # Add final page to buffer
            pg_buf[ctr] = (curr_blk_num, pg)

            # Only write if buffer contains data
            if any(item is not None for item in pg_buf):
                self.write_buffer_to_disk(True)  # Final write

            # Clear the buffer
            for i in range(len(pg_buf)):
                pg_buf[i] = None

            logger.debug("Completed processing final block")

        def _write_journal_tags(self, is_start: bool):
            """Write start or end tag to the journal file."""
            if is_start:
                self._journal._file_io.write_start_tag()
                self._journal._file_io.write_ct_bytes(self._journal.ct_bytes_to_write)
            else:
                self._journal._file_io.write_end_tag()

        def _update_metadata(self, new_g_pos: int, new_p_pos: int, ttl_bytes: int):
            """Update journal metadata."""
            self._journal._metadata.meta_get = new_g_pos
            self._journal._metadata.meta_put = new_p_pos
            self._journal._metadata.meta_sz = ttl_bytes
            self._journal._metadata.write(new_g_pos, new_p_pos, ttl_bytes)

        def _flush_and_update_status(self):
            """Flush journal data to disk and update status."""
            self._journal.journal_file.flush()
            os.fsync(self._journal.journal_file.fileno())
            logger.info(f"Change log written at time {get_cur_time()}")
            self._journal.status.wrt("Change log written")

        def wrt_cg_log_to_jrnl(self, r_cg_log: ChangeLog):
            """Write entire change log to journal."""
            logger.debug(f"Entering wrt_cg_log_to_jrnl with {len(r_cg_log.the_log)} blocks in change log")

            if not r_cg_log.cg_line_ct:
                return

            logger.info("Writing change log to journal")
            r_cg_log.print()

            self._journal.ttl_bytes_written = 0
            self._journal.ct_bytes_to_write = self.calculate_ct_bytes_to_write(r_cg_log)
            logger.debug(f"Calculated bytes to write: {self._journal.ct_bytes_to_write}")

            # Record start position
            start_pos = self._journal.journal_file.tell()
            logger.debug(f"Starting journal write at position: {start_pos}")

            self._write_journal_tags(True)  # Write start tag
            self._journal._file_io.wrt_cgs_to_jrnl(r_cg_log)
            logger.debug(f"Actual bytes written: {self._journal.ttl_bytes_written}")

            # Calculate end tag position and seek there
            end_tag_pos = start_pos + self._journal.ct_bytes_to_write
            if end_tag_pos >= u32Const.JRNL_SIZE.value:
                end_tag_pos = self._journal.META_LEN + (end_tag_pos - u32Const.JRNL_SIZE.value)
                logger.debug(f"End tag wraps around to position: {end_tag_pos}")
            else:
                logger.debug(f"End tag position without wrap: {end_tag_pos}")

            self._journal.journal_file.seek(end_tag_pos)
            self._write_journal_tags(False)  # Write end tag

            # Update metadata
            new_g_pos = self._journal.META_LEN
            new_p_pos = self._journal.journal_file.tell()
            ttl_bytes = self._journal.ct_bytes_to_write + self._journal.META_LEN

            self._update_metadata(new_g_pos, new_p_pos, ttl_bytes)
            self._flush_and_update_status()

            logger.debug(f"Exiting wrt_cg_log_to_jrnl. Wrote {self._journal.ttl_bytes_written} bytes. Final metadata - "
                         f"get: {self._journal._metadata.meta_get}, "
                         f"put: {self._journal._metadata.meta_put}, "
                         f"size: {self._journal._metadata.meta_sz}")

            r_cg_log.cg_line_ct = 0

        def process_changes(self, j_cg_log: ChangeLog):
            """Process all changes in the change log.

            Args:
                j_cg_log: The ChangeLog object containing changes to process.
            """
            if not j_cg_log.the_log:
                logger.debug("Change log is empty, nothing to process")
                return

            # Check for invalid block numbers before processing
            for block_num in j_cg_log.the_log:
                if block_num >= bNum_tConst.NUM_DISK_BLOCKS.value:
                    raise ValueError(f"Invalid block number: {block_num}")

            # Filter out blocks with empty change lists
            blocks = [(blk_num, changes) for blk_num, changes in j_cg_log.the_log.items() if changes]

            if not blocks:
                logger.debug("No non-empty change lists, nothing to process")
                return

            prev_block_num = SENTINEL_INUM
            current_page = None

            for blk_num, changes in blocks:
                prev_block_num, current_page = self._process_block(changes, prev_block_num, current_page)

            # Handle the last processed block
            if prev_block_num != SENTINEL_INUM:
                self._add_to_buffer(prev_block_num, current_page)

            # Final write to disk
            self.write_buffer_to_disk(True)

            # Clear the buffer
            self.pg_buf = [None] * self._journal.PAGE_BUFFER_SIZE

        def _process_block(self, changes: List[Change], prev_block_num: bNum_t, prev_page: Page) -> Tuple[bNum_t, Page]:
            """Process a list of changes for a block.

            Args:
                changes: List of Change objects to process.
                prev_block_num: The previous block number.
                prev_page: The previous Page object.

            Returns:
                A tuple containing the current block number and Page object.
            """
            current_block_num = prev_block_num
            current_page = prev_page

            for change in changes:
                if change.block_num != current_block_num:
                    self._handle_block_transition(current_block_num, current_page)
                    current_block_num, current_page = self._read_new_block(change.block_num)

                self._apply_change_to_page(change, current_page)

            return current_block_num, current_page

        def _process_last_block(self, cg: Change, curr_blk_num: bNum_t):
            """Process the final block in a series of changes."""
            logger.debug(f"Processing last block {curr_blk_num}")

            # Seek and read the last block
            disk_stream = self._journal.sim_disk.get_ds()
            seek_pos = curr_blk_num * u32Const.BLOCK_BYTES.value
            disk_stream.seek(seek_pos, 0)
            pg = Page()
            pg.dat = bytearray(disk_stream.read(u32Const.BLOCK_BYTES.value))

            # Write change to page
            self.wrt_cg_to_pg(cg, pg)

            # Check buffer state before adding new item
            current_count = self.count_buffer_items()
            assert current_count <= self._journal.PAGE_BUFFER_SIZE, (
                f"Buffer overflow: {current_count} items in size "
                f"{self._journal.PAGE_BUFFER_SIZE} buffer"
            )

            # Write to disk if buffer is full
            if current_count == self._journal.PAGE_BUFFER_SIZE:
                self.write_buffer_to_disk(False)  # Not final, just full
                self.pg_buf = [None] * self._journal.PAGE_BUFFER_SIZE
                current_count = 0

            # Add to buffer
            self.pg_buf[current_count] = (curr_blk_num, pg)

            # Only write to disk again if there's actually data to write
            if any(item is not None for item in self.pg_buf):
                self.write_buffer_to_disk(True)  # Final write

            # Clear the buffer
            self.pg_buf = [None] * self._journal.PAGE_BUFFER_SIZE

        def count_buffer_items(self) -> int:
            """Count non-None items in the buffer."""
            return sum(1 for item in self.pg_buf if item is not None)

        def write_buffer_to_disk(self, is_end: bool = False) -> bool:
            """Coordinate writing buffered pages to disk.

            This method manages the high-level process of writing buffered pages to disk:
            1. Iterates through the buffer
            2. Verifies CRC for each page
            3. Delegates actual disk writing to Journal.write_block_to_disk

            This method owns the buffer and understands its structure, while
            delegating physical I/O operations to the Journal class.

            Args:
                is_end: Whether this is the final write operation in the current sequence.
                       When True, ensures all buffered pages are written.

            Returns:
                bool: True if all writes were successful, False otherwise

            See Also:
                Journal.write_block_to_disk: Handles the actual disk I/O for individual blocks
            """
            """Coordinate writing buffered pages to disk."""
            logger.debug(f"Initiating buffer write (is_end={is_end})")

            pages_to_write = [item for item in self.pg_buf if item is not None]
            logger.debug(f"  {len(pages_to_write)} pages to write: {[item[0] for item in pages_to_write if item]}")

            if not pages_to_write and not is_end:
                logger.debug("  No pages to write, returning early")
                return True

            try:
                for i, (block_num, page) in enumerate(pages_to_write):
                    logger.debug(f"  Processing page {i + 1}/{len(pages_to_write)} (block {block_num})")

                    # Verify CRC
                    if not self._journal.verify_page_crc((block_num, page)):
                        logger.error(f"    CRC check failed for block {block_num}")
                        return False

                    # Delegate to Journal for actual write
                    self._journal.write_block_to_disk(block_num, page)

                # Clear the buffer
                self.pg_buf = [None] * self._journal.PAGE_BUFFER_SIZE
                logger.debug("  Buffer cleared after write operation")
                return True
            except Exception as e:
                logger.error(f"  Error during buffer write process: {e}")
                return False


if __name__ == "__main__":
    import sys

    print("Notice: The journal buffer management tests have been moved to the pytest suite.")
    print("To run these tests, please use one of the following commands:")
    print("  pytest tests/test_journal.py                  # Run all journal tests")
    print("  pytest tests/test_journal.py -k buffer        # Run only buffer management tests")
    print("  pytest tests/test_journal.py -v               # Run all tests with verbose output")

    if len(sys.argv) > 1:
        print("\nNote: Command-line arguments for individual test cases are no longer supported.")
        print("Please use pytest's built-in filtering and selection options instead.")

    sys.exit(0)