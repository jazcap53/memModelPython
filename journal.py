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
from typing import List, Dict, Tuple, Optional, BinaryIO
from collections import deque
from ajTypes import bNum_t, lNum_t, u32Const, bNum_tConst, SENTINEL_INUM
from ajCrc import AJZlibCRC
from ajUtils import get_cur_time, Tabber, format_hex_like_hexdump
from wipeList import WipeList
from change import Change, ChangeLog, Select
from myMemory import Page
import os
import io
from contextlib import contextmanager
from logging_config import get_logger


logger = get_logger(__name__)
end_tag_logger = get_logger('journal.end_tag')



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
    START_RW_AT_META_LEN = -1

    total_bytes_read = 0
    total_bytes_written = 0

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
        self._recently_purged = False

        # Counters for tracking I/O
        self.total_bytes_read = 0
        self.total_bytes_written = 0

        self.read_log = []

        # File initialization
        file_existed = os.path.exists(self.f_name)
        self.journal_file: BinaryIO = io.open(self.f_name, "rb+" if file_existed else "wb+")

        self.seek(0, 2)  # Go to end of file
        current_size = self.tell()
        if current_size < u32Const.JRNL_SIZE.value:
            remaining = u32Const.JRNL_SIZE.value - current_size
            self.write(b'\0' * remaining)
        self.seek(0)  # Reset to beginning

        # Verify file size
        self.seek(0, 2)  # Go to end
        actual_size = self.tell()
        self.seek(0)  # Reset to beginning
        if actual_size != u32Const.JRNL_SIZE.value:
            raise RuntimeError(f"Journal file size mismatch. Expected {u32Const.JRNL_SIZE.value}, got {actual_size}")

        self.seek(self.META_LEN)

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

    def calculate_end_tag_position(self, start_pos: int, data_size: int, include_header: bool = True) -> int:
        """Calculate the consistent position for an end tag.

        This method provides a unified calculation that can be used by both read and write operations
        to ensure consistency in end tag positioning. Handles multiple wraparounds if necessary.

        Args:
            start_pos: Starting position of the journal entry (where the START_TAG begins)
            data_size: Size of the data portion (ct_bytes_to_write)
            include_header: Whether to include the header size in calculation
                            (True for normal operation, False for special cases)

        Returns:
            The absolute file position where the end tag should be placed
        """
        # Header is START_TAG (8 bytes) + ct_bytes_to_write field (8 bytes)
        header_size = 16 if include_header else 0

        # Calculate end position
        end_tag_pos = start_pos + header_size + data_size

        # Handle wraparound, potentially multiple times
        while end_tag_pos >= u32Const.JRNL_SIZE.value:
            end_tag_pos = self.META_LEN + (end_tag_pos - u32Const.JRNL_SIZE.value)

        # Validate final position is within bounds
        assert self.META_LEN <= end_tag_pos < u32Const.JRNL_SIZE.value, \
            f"End tag position {end_tag_pos} outside valid range [{self.META_LEN}, {u32Const.JRNL_SIZE.value})"

        # Store this for debugging purposes
        self.end_tag_posn = end_tag_pos

        return end_tag_pos

    def read(self, size=-1):
        """Read from journal file with logging."""
        try:
            data = self.journal_file.read(size)
            bytes_read = len(data) if size == -1 else size
            self.total_bytes_read += bytes_read
            return data
        except IOError as e:
            raise

    def write(self, data):
        """Write to journal file with logging."""
        try:
            bytes_written = self.journal_file.write(data)
            self.total_bytes_written += bytes_written
            return bytes_written
        except IOError as e:
            raise

    def seek(self, offset, whence=0):
        """Seek in journal file with logging."""
        try:
            result = self.journal_file.seek(offset, whence)
            return result
        except IOError as e:
            raise

    def tell(self):
        """Return current file position."""
        return self.journal_file.tell()

    def flush(self):
        """Flush journal file."""
        return self.journal_file.flush()

    def close(self):
        return self.journal_file.close()

    def fileno(self):
        return self.journal_file.fileno()

    def reset_counters(self):
        """Reset byte counters."""
        self.total_bytes_read = 0
        self.total_bytes_written = 0

    def write_change_log_to_journal(self, r_cg_log: ChangeLog):
        """Write a change log to the journal. Public interface method."""
        if not r_cg_log.cg_line_ct:
            return

        self._change_log_handler._implement_journal_write(r_cg_log)

    def purge_jrnl(self, keep_going: bool, had_crash: bool):
        """Purge the journal, optionally handling crash recovery."""
        logger.info("Purging journal")

        if self.debug:
            return

        # Skip redundant purges unless this is a crash recovery
        if not had_crash and self._recently_purged:
            return

        self._file_io.reset_file()  # Reset file state before purging

        if self._is_journal_empty() and not had_crash:
            pass
        else:
            self._process_journal_changes(had_crash)

        self._reset_metadata()
        self._update_status(keep_going)
        self._recently_purged = True

        logger.info(f"Journal purge completed at time {get_cur_time()}")

    def set_wiper_dirty(self, b_num: bNum_t):
        """Mark a block as dirty in the wiper list.

        Args:
            b_num: The block number to mark as dirty
        """
        self.wipers.set_dirty(b_num)

    def get_file(self) -> BinaryIO:
        """Return the journal's file object."""
        return self.journal_file

    def _is_journal_empty(self) -> bool:
        """Check if the journal is empty."""
        return not any(self.blks_in_jrnl)

    def _process_journal_changes(self, had_crash: bool):
        """Process changes in the journal."""
        j_cg_log = ChangeLog()
        # self.rd_last_jrnl(j_cg_log)
        self.rd_last_jrnl(j_cg_log)

        self._log_change_summary(j_cg_log)

        if not j_cg_log.the_log:
            pass
        else:
            self._apply_changes(j_cg_log)

        self._clear_journal_state()

    def _log_change_summary(self, j_cg_log: ChangeLog):
        """Log a summary of changes in the journal."""
        for block, changes in j_cg_log.the_log.items():
            pass

    def _apply_changes(self, j_cg_log: ChangeLog):
        """Apply changes from the journal to the disk."""
        self._change_log_handler.process_changes(j_cg_log)

    def _process_final_change(self, j_cg_log: ChangeLog, ctr: int, curr_blk_num: bNum_t, pg: Page):
        """Process the final change in a series."""
        if curr_blk_num is not None and curr_blk_num in j_cg_log.the_log and j_cg_log.the_log[curr_blk_num]:
            cg = j_cg_log.the_log[curr_blk_num][-1]
            self._change_log_handler.r_and_wb_last(cg, self.pg_buf, ctr, curr_blk_num, pg)
        else:
            pass

    def _clear_journal_state(self):
        """Clear the journal state after processing changes."""
        self.blks_in_jrnl = [False] * bNum_tConst.NUM_DISK_BLOCKS.value
        self.change_log.the_log.clear()

    def _reset_metadata(self):
        """Reset the journal metadata."""
        self._metadata.meta_get = self.START_RW_AT_META_LEN
        self._metadata.meta_put = 24
        self._metadata.meta_sz = 0
        self._metadata.write(-1, 24, 0)

    def _update_status(self, keep_going: bool):
        """Update the status after purging the journal."""
        self.status.wrt("Purged journal" if keep_going else "Finishing")

    def is_in_jrnl(self, b_num: bNum_t) -> bool:
        """Check if a block number is currently in the journal."""
        return self.blks_in_jrnl[b_num]

    def do_wipe_routine(self, b_num: bNum_t, p_f_m):
        """Perform the wipe routine for a given block."""
        if self.wipers.is_dirty(b_num) or self.wipers.is_ripe():
            logger.info("Saving change log and purging journal")

            p_f_m.do_store_inodes()
            p_f_m.do_store_free_list()
            self.write_change_log_to_journal(self.change_log)
            self.purge_jrnl(True, False)
            self.wipers.clear_array()

            logger.info("Wipe routine completed")

    def _read_journal_metadata(self):
        """Read and validate journal metadata.

        This is a bridge method to the newer implementation.

        Returns:
            int or None: The position to start reading from, or None if no valid data
        """
        return self._read_journal_metadata_new()

    def _verify_journal_tags(self, start_tag: int, end_tag: int):
        """Verify journal tags match expected values.

        This is a bridge method to the newer implementation.
        """
        return self._verify_journal_tags_new(start_tag, end_tag)

    def _process_journal_entry(self, ttl_bytes):
        """Process the journal entry after reading."""
        self._verify_bytes_read()

    def rd_last_jrnl(self, r_j_cg_log: ChangeLog):
        """Bridge method that calls the new implementation."""
        return self.rd_last_jrnl_new(r_j_cg_log)

    def rd_jrnl(self, r_j_cg_log: ChangeLog, start_pos: int) -> Tuple[int, int, int]:
        """Read journal contents from a given position.

        Args:
            r_j_cg_log: Change log to populate
            start_pos: Position to start reading from

        Returns:
            Tuple of (start_tag, end_tag, bytes_read)

        Raises:
            ValueError: If start_pos is invalid
        """
        # Validate start position
        if start_pos < self.META_LEN or start_pos >= u32Const.JRNL_SIZE.value:
            raise ValueError(f"Invalid start position: {start_pos}")

        self.seek(start_pos)

        # Read start tag and convert to integer
        start_tag_bytes = self._file_io.rd_field(8)
        start_tag = from_bytes_64bit(start_tag_bytes)

        # Read bytes-to-write count and convert to integer
        ct_bytes_bytes = self._file_io.rd_field(8)
        ct_bytes_to_write = from_bytes_64bit(ct_bytes_bytes)
        self.ct_bytes_to_write = ct_bytes_to_write

        # Read changes (now passing an integer)
        bytes_read = self._read_changes(r_j_cg_log, ct_bytes_to_write)

        # Calculate and seek to end tag position
        end_tag_pos = self.calculate_end_tag_position(start_pos, ct_bytes_to_write)
        self.seek(end_tag_pos)

        # Read end tag and convert to integer
        end_tag_bytes = self._file_io.rd_field(8)
        end_tag = from_bytes_64bit(end_tag_bytes)

        # Verify tags
        self._verify_journal_tags(start_tag, end_tag)

        return start_tag, end_tag, bytes_read

    def _read_start_tag(self) -> int:
        """Read and return the start tag from the journal file."""
        tag_bytes = self._read_with_log(8)
        return from_bytes_64bit(tag_bytes)

    def _read_ct_bytes_to_write(self) -> int:
        """Read and return the count of bytes to write."""
        bytes_data = self._read_with_log(8)
        return from_bytes_64bit(bytes_data)

    def _calculate_end_tag_position(self, start_pos: int, ct_bytes_to_write: int) -> int:
        """Calculate position of end tag."""
        return self.calculate_end_tag_position(start_pos, ct_bytes_to_write)

    def _read_changes(self, r_j_cg_log: ChangeLog, ct_bytes_to_write: int) -> int:
        """Read changes from the journal and populate the change log.

        This is a bridge method to the newer implementation that reads all data at once
        instead of processing it line by line, which helps avoid position errors.

        Args:
            r_j_cg_log: Change log to populate
            ct_bytes_to_write: Number of bytes to read

        Returns:
            Number of bytes read
        """
        # Get the current position to pass to the new implementation
        start_pos = self.tell()
        return self._read_changes_new(r_j_cg_log, ct_bytes_to_write, start_pos)

    def _check_journal_end(self, bytes_read: int, ct_bytes_to_write: int) -> bool:
        """Check if we've read all the bytes we need or reached a genuine end."""
        if bytes_read >= ct_bytes_to_write:
            return True

        current_pos = self.tell()
        if current_pos + 16 > u32Const.JRNL_SIZE.value:
            # Before returning True, check if we should wrap
            remaining_bytes = ct_bytes_to_write - bytes_read
            if remaining_bytes > 0:
                self.seek(self.META_LEN)
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
        if self.tell() + 8 > u32Const.JRNL_SIZE.value:
            return None, 0

        selector_data = self.read(8)
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
            if self.tell() + u32Const.BYTES_PER_LINE.value > u32Const.JRNL_SIZE.value:
                break

            line_data = self.read(u32Const.BYTES_PER_LINE.value)
            data_bytes_read += u32Const.BYTES_PER_LINE.value
            cg.new_data.append(line_data)

        return data_bytes_read

    def _read_crc_and_padding(self, bytes_read: int, ct_bytes_to_write: int) -> int:
        """Read CRC and padding if there's enough space."""
        if bytes_read + 8 <= ct_bytes_to_write:
            self._file_io.rd_field(4)  # Read CRC
            self._file_io.rd_field(4)  # Read padding
            bytes_read += 8
        return bytes_read

    def _read_end_tag(self) -> int:
        """Read and return the end tag."""
        current_pos = self.tell()
        tag_bytes = self._read_with_log(8)
        tag_value = from_bytes_64bit(tag_bytes)
        return tag_value

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
            # Seek to correct position
            self.sim_disk.get_ds().seek(block_num * u32Const.BLOCK_BYTES.value)

            # Check if block is dirty
            if self.wipers.is_dirty(block_num):
                # Write zeros for dirty blocks
                self.sim_disk.get_ds().write(b'\0' * u32Const.BLOCK_BYTES.value)
            else:
                # Write actual page data
                self.sim_disk.get_ds().write(page.dat)
        except IOError as e:
            raise

    def _verify_bytes_read(self):
        """Verify that the number of bytes read matches the expected count.

        This is a bridge method to the newer implementation.
        """
        self._verify_bytes_read_new()

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
            raise

        finally:
            # Clean up files
            for file in [disk_file, journal_file, free_list_file, inode_file, status_file]:
                if os.path.exists(file):
                    try:
                        os.remove(file)
                    except Exception as e:
                        pass

    def _read_with_log(self, size: int) -> bytes:
        """Read bytes using ajTypes functions while maintaining the read log.

        This is a bridge method to the newer implementation.

        Args:
            size: Number of bytes to read (typically 4 or 8)

        Returns:
            The read data as bytes
        """
        return self._read_with_log_new(size)

    def _debug_journal_layout(self):
        """Debug helper to dump journal layout information."""
        meta_get = self.meta_get

        # Calculate key positions
        start_pos = meta_get if meta_get != -1 else self.META_LEN
        changes_start = start_pos + 16
        calculated_end_tag_pos = changes_start + self.ct_bytes_to_write
        if calculated_end_tag_pos >= u32Const.JRNL_SIZE.value:
            calculated_end_tag_pos = self.META_LEN + (calculated_end_tag_pos - u32Const.JRNL_SIZE.value)

        # Look for actual end tag position
        current_pos = self.tell()
        actual_pos = self._find_end_tag_position()
        if actual_pos is not None:
            pass
        self.seek(current_pos)

    def _find_end_tag_position(self):
        """Find position of the END_TAG in the journal file."""
        # Save current position
        current_pos = self.tell()

        try:
            # To prevent excessive scanning, only look in the likely area
            search_start = max(0, self.meta_put - 1000)
            search_end = min(u32Const.JRNL_SIZE.value, self.meta_put + 1000)

            self.seek(search_start)

            # Read the file in chunks and search for the end tag
            while self.tell() < search_end:
                chunk = self.read(1024)
                if len(chunk) < 8:
                    break

                for i in range(len(chunk) - 7):
                    value = int.from_bytes(chunk[i:i + 8], byteorder='little')
                    if value == self.END_TAG:
                        position = search_start + i
                        end_tag_logger.debug(f"Found END_TAG at position {position}")
                        return position

                # Move back 7 bytes to handle end tag across chunk boundaries
                if len(chunk) >= 7:
                    self.seek(self.tell() - 7)

        finally:
            # Restore original position
            self.seek(current_pos)

        return None

    def _read_journal_metadata_new(self):
        """Read metadata from journal file with position tracking.

        Returns:
            int or None: The position to start reading from, or None if no valid data
        """
        original_pos = self.tell()
        try:
            self.seek(0)
            self.read_log.append((0, 24))  # Log the read accurately

            meta_get_bytes = self.read(8)
            meta_put_bytes = self.read(8)
            meta_sz_bytes = self.read(8)

            meta_get = from_bytes_64bit(meta_get_bytes)
            meta_put = from_bytes_64bit(meta_put_bytes)
            meta_sz = from_bytes_64bit(meta_sz_bytes)

            # Set the properties to maintain backward compatibility
            self.meta_get = meta_get
            self.meta_put = meta_put
            self.meta_sz = meta_sz

            # Special case: meta_get of -1 means start at META_LEN
            # (beginning of data section)
            if meta_get == self.START_RW_AT_META_LEN:
                return self.META_LEN

            # Validate meta_get is in valid range
            if meta_get < self.META_LEN or meta_get >= u32Const.JRNL_SIZE.value:
                logger.warning(f"Invalid meta_get value: {meta_get}")
                return None

            # Valid meta_get value
            return meta_get

        finally:
            # Restore original position if needed
            if original_pos != self.tell():
                self.seek(original_pos)

    def rd_last_jrnl_new(self, r_j_cg_log: ChangeLog):
        """Read the last journal entry into a change log with proper position tracking."""
        # Clear read log before starting
        self.read_log = []

        try:
            # Use meaningful constants instead of magic numbers
            METADATA_SIZE = 24  # Size of metadata (meta_get, meta_put, meta_sz combined)
            START_TAG_SIZE = 8  # Size of the START_TAG field
            BYTES_COUNT_SIZE = 8  # Size of the ct_bytes_to_write field
            HEADER_SIZE = START_TAG_SIZE + BYTES_COUNT_SIZE  # Combined size of START_TAG and bytes count
            END_TAG_SIZE = 8  # Size of the END_TAG field

            # Read metadata (position 0, METADATA_SIZE bytes)
            meta_get, meta_put, meta_sz = self._metadata.read()

            # Determine start position for journal read
            start_pos = self.META_LEN if meta_get == -1 else meta_get

            # Read start tag using read_64bit
            self.seek(start_pos)
            start_tag = read_64bit(self.journal_file)
            self.read_log.append((start_pos, START_TAG_SIZE))

            # Read bytes count using read_64bit
            bytes_count_pos = start_pos + START_TAG_SIZE
            self.seek(bytes_count_pos)
            ct_bytes_to_write = read_64bit(self.journal_file)
            self.ct_bytes_to_write = ct_bytes_to_write
            self.read_log.append((bytes_count_pos, BYTES_COUNT_SIZE))

            # Read changes (ct_bytes_to_write bytes)
            changes_start_pos = start_pos + HEADER_SIZE
            self.seek(changes_start_pos)
            bytes_read = self._read_changes(r_j_cg_log, ct_bytes_to_write)
            self.read_log.append((changes_start_pos, ct_bytes_to_write))

            # Calculate end tag position
            end_tag_pos = self.calculate_end_tag_position(
                start_pos,
                ct_bytes_to_write
            )

            # Read end tag using read_64bit
            self.seek(end_tag_pos)
            end_tag = read_64bit(self.journal_file)
            self.read_log.append((end_tag_pos, END_TAG_SIZE))

            # Verify start and end tags
            try:
                self._verify_journal_tags(start_tag, end_tag)
            except ValueError as e:
                if "Invalid end tag" in str(e):
                    if self.debug:
                        end_tag_logger.error(f"Error in rd_last_jrnl: {e}")
                else:
                    if self.debug:
                        logger.error(f"Error in rd_last_jrnl: {e}")
                raise  # Re-raise the exception

            # Process the journal entry
            self._process_journal_entry(bytes_read)

            return bytes_read
        except Exception as e:
            if "Invalid end tag" not in str(e) and self.debug:
                logger.error(f"Error in rd_last_jrnl_new: {e}")
            raise  # Re-raise all exceptions

    def _read_changes_new(self, r_j_cg_log: ChangeLog, ct_bytes_to_write: int, start_pos: int) -> int:
        """Read changes data with safer positioning and improved error handling.

        This implementation reads all data at once to avoid position errors that
        could occur when reading incrementally, especially when dealing with
        wrapped-around journal entries.

        Args:
            r_j_cg_log: Change log to populate with read data
            ct_bytes_to_write: Number of bytes to read
            start_pos: Starting position for the changes section

        Returns:
            Number of bytes that were read
        """
        # Read all change data at once to avoid position errors
        all_data = bytearray()
        bytes_remaining = ct_bytes_to_write
        current_pos = start_pos

        while bytes_remaining > 0:
            # Calculate bytes to read before potential wraparound
            bytes_to_end = u32Const.JRNL_SIZE.value - current_pos
            bytes_to_read = min(bytes_remaining, bytes_to_end)

            # Read chunk using existing file I/O methods
            self.seek(current_pos)
            chunk = self._file_io.rd_field(bytes_to_read)
            if not chunk:  # If we hit EOF unexpectedly
                break

            all_data.extend(chunk)
            bytes_remaining -= len(chunk)

            # Wrap around if needed
            if bytes_remaining > 0:
                current_pos = self.META_LEN

        # Process the data in memory
        data_pos = 0
        while data_pos < len(all_data):
            # Ensure we have enough data for block number and timestamp
            if data_pos + 16 > len(all_data):
                break

            # Read block number
            block_num_bytes = all_data[data_pos:data_pos + 8]
            b_num = from_bytes_64bit(block_num_bytes)
            if self.debug:
                print(f"Reading block number: {b_num}")
            data_pos += 8

            # Validate block number
            if b_num >= bNum_tConst.NUM_DISK_BLOCKS.value:
                logger.warning(f"Invalid block number: {b_num}")
                break

            # Read timestamp
            timestamp_bytes = all_data[data_pos:data_pos + 8]
            timestamp = from_bytes_64bit(timestamp_bytes)
            if self.debug:
                print(f"Reading timestamp: {timestamp}")
            data_pos += 8

            # Create change object
            cg = Change(b_num)
            cg.time_stamp = timestamp

            # Read selectors and data
            try:
                while data_pos < len(all_data):
                    # Read selector
                    if data_pos + 8 > len(all_data):
                        break

                    selector_bytes = all_data[data_pos:data_pos + 8]
                    selector = Select.from_bytes(selector_bytes)
                    data_pos += 8
                    cg.selectors.append(selector)

                    # Process set bits and read data
                    for i in range(63):  # Bits 0-62
                        if not selector.is_set(i):
                            continue

                        if data_pos + u32Const.BYTES_PER_LINE.value > len(all_data):
                            break

                        line_data = all_data[data_pos:data_pos + u32Const.BYTES_PER_LINE.value]
                        data_pos += u32Const.BYTES_PER_LINE.value
                        cg.new_data.append(line_data)

                    # If this was the last selector, we're done with this change
                    if selector.is_last_block():
                        break

                # Skip CRC and padding (8 bytes)
                if data_pos + 8 <= len(all_data):
                    data_pos += 8

                # Add the change to the log if it has at least one selector
                if cg.selectors:
                    r_j_cg_log.add_to_log(cg)

            except Exception as e:
                logger.error(f"Error processing change: {e}")
                raise  # Propagate exception as requested

        return ct_bytes_to_write  # Return expected bytes

    def _verify_journal_tags_new(self, start_tag: int, end_tag: int):
        """Verify journal tags more robustly.

        This method checks that both start and end tags match expected values
        with better error reporting.
        """
        if start_tag != self.START_TAG:
            if self.debug:
                logger.error(f"Start tag verification failed. Expected {self.START_TAG:x}, got {start_tag:x}")
            raise ValueError(f"Invalid start tag: {start_tag:x}")

        if end_tag != self.END_TAG:
            if self.debug:
                end_tag_logger.error(f"End tag verification failed. Expected {self.END_TAG:x}, got {end_tag:x}")
            raise ValueError(f"Invalid end tag: {end_tag:x}")

    def _read_with_log_new(self, size: int) -> bytes:
        """Read bytes using ajTypes functions while maintaining the read log.

        Args:
            size: Number of bytes to read (typically 4 or 8)

        Returns:
            The read data as bytes
        """
        current_position = self.tell()

        # Use ajTypes functions for actual reading
        if size == 8:
            value = read_64bit(self.journal_file)
            data = to_bytes_64bit(value)
        elif size == 4:
            value = read_32bit(self.journal_file)
            data = struct.pack('<I', value)
        else:
            data = self.journal_file.read(size)

        # Log the read operation
        self.total_bytes_read += len(data)
        # Only append to log if we actually read anything
        if data:
            self.read_log.append((current_position, len(data)))

        return data

    def _process_journal_entry_new(self, bytes_read):
        """Process the journal entry after reading it.

        This version only logs verification instead of raising assertions
        to better match original behavior.
        """
        try:
            self._verify_bytes_read()
        except AssertionError as e:
            logger.warning(f"Read verification warning: {e}")

    def _verify_bytes_read_new(self):
        """Verify journal reads with more precise logging and handling.

        This method compares actual versus expected read patterns with
        improved diagnostics and matching.
        """
        # Get current metadata for calculation
        meta_get = self.meta_get

        # Calculate end tag position
        end_tag_pos = self.calculate_end_tag_position(
            meta_get,
            self.ct_bytes_to_write
        )

        expected_reads = [
            (0, 24),  # Metadata read
            (meta_get, 8),  # Start tag
            (meta_get + 8, 8),  # ct_bytes_to_write field
            (meta_get + 16, self.ct_bytes_to_write),  # Actual changes
            (end_tag_pos, 8)  # End tag
        ]

        # Actual read pattern (take last 5 entries if available)
        actual_reads = self.read_log[-len(expected_reads):] if len(self.read_log) >= len(
            expected_reads) else self.read_log

        # Compare and assert like the original method
        for i, (expected, actual) in enumerate(zip(expected_reads, actual_reads)):
            if expected != actual:
                logger.debug(f"Read {i} mismatch: expected {expected}, got {actual}")

        assert actual_reads == expected_reads, (
            f"Read mismatch: expected {expected_reads}, got {actual_reads}"
        )


    class _Metadata:
        """Handles journal metadata operations."""

        def __init__(self, journal_instance: 'Journal'):
            self._journal = journal_instance
            self.meta_get = 0
            self.meta_put = 0
            self.meta_sz = 0

        def read(self):
            """Read metadata using journal's logging capability."""
            self._journal.seek(0)
            try:
                # Read all 24 bytes at once for correct logging
                all_metadata = self._journal._read_with_log(24)

                # Extract the values from the single read
                meta_get = from_bytes_64bit(all_metadata[0:8])
                meta_put = from_bytes_64bit(all_metadata[8:16])
                meta_sz = from_bytes_64bit(all_metadata[16:24])

                self.meta_get = meta_get
                self.meta_put = meta_put
                self.meta_sz = meta_sz

                return meta_get, meta_put, meta_sz
            except Exception as e:
                return self._journal.START_RW_AT_META_LEN, 24, 0

        def write(self, new_g_pos: int, new_p_pos: int, u_ttl_bytes_written: int):
            """Write metadata to journal file."""
            # Save current position
            original_position = self._journal.tell()

            try:
                # Seek to start of file for metadata update
                self._journal.seek(0)
                metadata = struct.pack('<qqq', new_g_pos, new_p_pos, u_ttl_bytes_written)
                self._journal.write(metadata)

                # Update instance attributes
                self.meta_get = new_g_pos
                self.meta_put = new_p_pos
                self.meta_sz = u_ttl_bytes_written
            finally:
                # Restore original position
                self._journal.seek(original_position)

        def init(self):
            """Initialize metadata to default values."""
            rd_pt = -1
            wrt_pt = 24
            bytes_stored = 0
            self._journal.seek(0)
            self._journal.write(struct.pack('<qqq', rd_pt, wrt_pt, bytes_stored))
            self.meta_get = rd_pt
            self.meta_put = wrt_pt
            self.meta_sz = bytes_stored

    class _FileIO:
        """Handles file I/O operations for the journal."""

        def __init__(self, journal_instance: 'Journal'):
            self._journal = journal_instance

        def wrt_field(self, data: bytes, dat_len: int, do_ct: bool) -> int:
            """Write a field to the journal file."""
            bytes_written = 0
            p_pos = self._journal.tell()
            buf_sz = u32Const.JRNL_SIZE.value
            end_pt = p_pos + dat_len

            if end_pt > buf_sz:
                overflow_bytes = end_pt - buf_sz
                bytes_until_end = dat_len - overflow_bytes

                if dat_len == 8:  # 64-bit value
                    # Write the first part
                    self._journal.write(data[:bytes_until_end])
                    bytes_written += bytes_until_end
                    if do_ct:
                        self._journal.ttl_bytes_written += bytes_until_end

                    # Move to the correct position after wraparound
                    self._journal.seek(self._journal.META_LEN)

                    # Write the remaining part
                    self._journal.write(data[bytes_until_end:])
                    bytes_written += overflow_bytes
                    if do_ct:
                        self._journal.ttl_bytes_written += overflow_bytes

                elif dat_len == 4:  # 32-bit value
                    value = int.from_bytes(data, byteorder='little')
                    write_32bit(self._journal.journal_file, value & ((1 << (bytes_until_end * 8)) - 1))
                    bytes_written += bytes_until_end
                    if do_ct:
                        self._journal.ttl_bytes_written += bytes_until_end
                    self._journal.seek(self._journal.META_LEN)
                    write_32bit(self._journal.journal_file, value >> (bytes_until_end * 8))
                    bytes_written += overflow_bytes
                    if do_ct:
                        self._journal.ttl_bytes_written += overflow_bytes
            else:
                # Add this new case
                bytes_written = self._journal.write(data)
                if do_ct:
                    self._journal.ttl_bytes_written += bytes_written
                return bytes_written

        def rd_field(self, dat_len: int) -> bytes:
            """Read a field from the journal file.

            Args:
                dat_len: Number of bytes to read

            Returns:
                bytes: Data read from journal file
            """
            g_pos = self._journal.tell()
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

        def _read_without_wraparound(self, dat_len: int) -> bytes:
            """Read data that fits within the current journal space."""
            file_obj = self._journal.get_file()

            if dat_len == 8:
                value = read_64bit(file_obj)
                return to_bytes_64bit(value)
            elif dat_len == 4:
                value = read_32bit(file_obj)
                return to_bytes_32bit(value)
            else:
                # For other lengths, read in chunks of appropriate size
                data = bytearray()
                remaining = dat_len
                while remaining > 0:
                    if remaining >= 8:
                        value = read_64bit(file_obj)
                        data.extend(to_bytes_64bit(value))
                        remaining -= 8
                    elif remaining >= 4:
                        value = read_32bit(file_obj)
                        data.extend(to_bytes_32bit(value))
                        remaining -= 4
                    else:
                        # Read remaining bytes one at a time
                        data.extend(to_bytes_32bit(read_32bit(file_obj))[:remaining])
                        remaining = 0
                return bytes(data)

        def _read_64bit_wraparound(self, under: int) -> bytes:
            """Read a 64-bit value that wraps around in the journal."""
            file_obj = self._journal.get_file()

            # Read low bits
            low_value = read_64bit(file_obj)
            self._update_bytes_read(under)

            # Read high bits from start of data section
            self._journal.seek(self._journal.META_LEN)
            high_value = read_64bit(file_obj)
            self._update_bytes_read(8 - under)

            # Combine values
            combined_value = (high_value << (under * 8)) | low_value
            return to_bytes_64bit(combined_value)

        def _read_32bit_wraparound(self, under: int) -> bytes:
            """Read a 32-bit value that wraps around in the journal."""
            file_obj = self._journal.get_file()

            # Read low bits
            low_value = read_32bit(file_obj)
            self._update_bytes_read(under)

            # Read high bits from start of data section
            self._journal.seek(self._journal.META_LEN)
            high_value = read_32bit(file_obj)
            self._update_bytes_read(4 - under)

            # Combine values
            combined_value = (high_value << (under * 8)) | low_value
            return to_bytes_32bit(combined_value)

        def _read_generic_wraparound(self, under: int, over: int) -> bytes:
            """Read generic data that wraps around in the journal."""
            file_obj = self._journal.get_file()
            data = bytearray()

            # Read first part
            remaining = under
            while remaining > 0:
                if remaining >= 8:
                    value = read_64bit(file_obj)
                    data.extend(to_bytes_64bit(value))
                    remaining -= 8
                elif remaining >= 4:
                    value = read_32bit(file_obj)
                    data.extend(to_bytes_32bit(value))
                    remaining -= 4
                else:
                    data.extend(to_bytes_32bit(read_32bit(file_obj))[:remaining])
                    remaining = 0
            self._update_bytes_read(under)

            # Read second part from start of data section
            self._journal.seek(self._journal.META_LEN)
            remaining = over
            while remaining > 0:
                if remaining >= 8:
                    value = read_64bit(file_obj)
                    data.extend(to_bytes_64bit(value))
                    remaining -= 8
                elif remaining >= 4:
                    value = read_32bit(file_obj)
                    data.extend(to_bytes_32bit(value))
                    remaining -= 4
                else:
                    data.extend(to_bytes_32bit(read_32bit(file_obj))[:remaining])
                    remaining = 0
            self._update_bytes_read(over)

            return bytes(data)

        def _update_bytes_read(self, count: int):
            """Update the total bytes read counter."""
            self._journal.total_bytes_read += count

        def advance_strm(self, length: int):
            """Advance the file stream position, handling wraparound."""
            new_pos = self._journal.tell() + length
            if new_pos >= u32Const.JRNL_SIZE.value:
                new_pos -= u32Const.JRNL_SIZE.value
                new_pos += self._journal.META_LEN
            self._journal.seek(new_pos)

        def reset_file(self):
            """Reset the journal file to initial state."""
            self._journal.close()
            self._journal.journal_file = open(self._journal.f_name, "rb+")
            self._journal.seek(0)

        def write_start_tag(self):
            """Write the start tag to the journal file."""
            write_64bit(self._journal.journal_file, self._journal.START_TAG)

        def write_end_tag(self):
            """Write the end tag to the journal file."""
            current_pos = self._journal.tell()
            write_64bit(self._journal.journal_file, self._journal.END_TAG)
            after_pos = self._journal.tell()
            if self._journal.debug:
                logger.debug(f"Writing end tag at position {current_pos}, "
                             f"new position {after_pos}")

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
            self.wrt_field(to_bytes_64bit(cg.block_num), 8, True)
            self._journal.blks_in_jrnl[cg.block_num] = True
            self.wrt_field(to_bytes_64bit(cg.time_stamp), 8, True)

        def _write_change_data(self, cg: Change) -> bytearray:
            """Write the data for a change and return the accumulated page data."""
            page_data = bytearray(u32Const.BYTES_PER_PAGE.value)

            for selector in cg.selectors:
                self._write_selector_and_data(selector, cg, page_data)

            return page_data

        def _write_selector_and_data(self, selector: Select, cg: Change, page_data: bytearray):
            """Write a selector and its associated data."""
            selector_bytes = selector.to_bytes()

            if self._journal.debug:
                print(f"Writing selector: {selector.value:016x}, bytes: {':'.join(f'{b:02x}' for b in selector_bytes)}")

            self.wrt_field(selector_bytes, 8, True)

            for i in range(63):  # Process up to 63 lines (excluding MSB)
                if not selector.is_set(i):
                    continue

                self._write_data_line(i, cg, page_data)

        def _write_data_line(self, line_num: int, cg: Change, page_data: bytearray):
            """Write a single line of data."""
            if not cg.new_data:
                return

            data = cg.new_data.popleft()
            data_bytes = data if isinstance(data, bytes) else bytes(data)
            self.wrt_field(data_bytes, u32Const.BYTES_PER_LINE.value, True)

            start = line_num * u32Const.BYTES_PER_LINE.value
            end = start + u32Const.BYTES_PER_LINE.value
            page_data[start:end] = data_bytes

        def _write_change_footer(self, page_data: bytearray):
            """Write the CRC and padding for a change."""
            crc = AJZlibCRC.get_code(page_data[:-4], u32Const.BYTES_PER_PAGE.value - 4)
            self.wrt_field(struct.pack('<I', crc), 4, True)
            self.wrt_field(b'\0\0\0\0', 4, True)

        def _finalize_journal_write(self):
            """Finalize the journal write operation."""
            self._journal.flush()
            os.fsync(self._journal.fileno())  # Ensure data is written to disk

        @staticmethod
        def _crc_check_pg(p_pr: Tuple[bNum_t, Page]) -> bool:
            """Verify the CRC of a page."""
            block_num, page = p_pr

            stored_crc = int.from_bytes(page.dat[-u32Const.CRC_BYTES.value:], 'little')
            calculated_crc = AJZlibCRC.get_code(page.dat[:-u32Const.CRC_BYTES.value],
                                                u32Const.BYTES_PER_PAGE.value - u32Const.CRC_BYTES.value)

            if stored_crc != calculated_crc:
                return False

            return True

    class _ChangeLogHandler:
        """Manages change log operations for the journal."""

        def __init__(self, journal_instance: 'Journal'):
            self._journal = journal_instance
            self.pg_buf: List[Optional[Tuple[int, Page]]] = [
                                                                None] * journal_instance.PAGE_BUFFER_SIZE  # Make this an instance attribute

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
                        break
                    temp = cg.new_data.popleft()
                    start = lin_num * u32Const.BYTES_PER_LINE.value
                    end = (lin_num + 1) * u32Const.BYTES_PER_LINE.value
                    pg.dat[start:end] = temp

            except NoSelectorsAvailableError:
                pass

            # Calculate and write CRC
            crc = AJZlibCRC.get_code(pg.dat[:-4], u32Const.BYTES_PER_PAGE.value - 4)
            pg.dat[-4:] = AJZlibCRC.wrt_bytes_little_e(crc, pg.dat[-4:], 4)

            logger.debug("Finished writing change to page")

        def rd_and_wrt_back(self, j_cg_log: ChangeLog, pg_buf: List, buf_page_count: int,
                            prev_blk_num: bNum_t, curr_blk_num: bNum_t, pg: Page):
            """Read changes from log and write them back to disk."""
            if not j_cg_log.the_log:
                return buf_page_count, prev_blk_num, curr_blk_num, pg

            try:
                blocks = list(j_cg_log.the_log.items())

                # Process all blocks except the last one
                for i in range(len(blocks) - 1):
                    blk_num, changes = blocks[i]

                    for cg in changes:
                        curr_blk_num = cg.block_num

                        if curr_blk_num != prev_blk_num or prev_blk_num == SENTINEL_INUM:
                            if prev_blk_num != SENTINEL_INUM:
                                pg_buf[buf_page_count] = (prev_blk_num, pg)
                                buf_page_count += 1

                                if buf_page_count == self._journal.PAGE_BUFFER_SIZE:
                                    self.write_buffer_to_disk(False)  # Not the end of processing
                                    buf_page_count = 0

                            # Seek and read new block
                            self._journal.sim_disk.get_ds().seek(curr_blk_num * u32Const.BLOCK_BYTES.value)

                            pg = Page()
                            pg.dat = bytearray(self._journal.sim_disk.get_ds().read(u32Const.BLOCK_BYTES.value))

                            prev_blk_num = curr_blk_num

                        self.wrt_cg_to_pg(cg, pg)

                # Handle the last processed block (if any)
                if len(blocks) > 1 and prev_blk_num != SENTINEL_INUM:
                    pg_buf[buf_page_count] = (prev_blk_num, pg)
                    buf_page_count += 1

                return buf_page_count, prev_blk_num, curr_blk_num, pg

            except Exception as e:
                raise

        def r_and_wb_last(self, cg: Change, pg_buf: List, ctr: int,
                          curr_blk_num: bNum_t, pg: Page):
            """Process the final change and ensure proper buffer handling."""
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
            self._journal.flush()
            os.fsync(self._journal.fileno())
            self._journal.status.wrt("Change log written")

        def _implement_journal_write(self, r_cg_log: ChangeLog):
            """Implement the actual journal write operation. Internal use only."""
            if not r_cg_log.cg_line_ct:
                return

            logger.debug("Writing change log to journal")

            self._journal.ttl_bytes_written = 0
            self._journal.ct_bytes_to_write = self.calculate_ct_bytes_to_write(r_cg_log)

            # Record start position
            start_pos = self._journal.tell()

            # Write start tag and bytes count using write_64bit
            write_64bit(self._journal.journal_file, self._journal.START_TAG)
            write_64bit(self._journal.journal_file, self._journal.ct_bytes_to_write)

            # Record position after writing header
            data_start_pos = self._journal.tell()

            self._journal._file_io.wrt_cgs_to_jrnl(r_cg_log)

            # Calculate end tag position more explicitly
            # The offset is 16 (START_TAG + ct_bytes_to_write fields) plus the data
            end_tag_pos = self._journal.calculate_end_tag_position(
                start_pos,
                self._journal.ct_bytes_to_write
            )

            # Store the end tag position for later validation
            self._journal.end_tag_posn = end_tag_pos

            # Use the stored position for seeking
            self._journal.seek(self._journal.end_tag_posn)

            # Verify position before writing end tag
            actual_end_pos = self._journal.tell()
            if actual_end_pos != end_tag_pos:
                logger.error(f"End tag position mismatch: expected {end_tag_pos}, got {actual_end_pos}")

            # Write end tag
            write_64bit(self._journal.journal_file, self._journal.END_TAG)

            # Update metadata
            new_g_pos = self._journal.META_LEN
            new_p_pos = self._journal.tell()
            ttl_bytes = self._journal.ct_bytes_to_write + self._journal.META_LEN

            self._update_metadata(new_g_pos, new_p_pos, ttl_bytes)
            self._flush_and_update_status()

            logger.debug(f"Change log written at time {get_cur_time()}")

            # Reset the recently purged flag in the parent Journal class
            self._journal._recently_purged = False

            r_cg_log.cg_line_ct = 0

        def process_changes(self, j_cg_log: ChangeLog):
            """Process all changes in the change log.

            Args:
                j_cg_log: The ChangeLog object containing changes to process.
            """
            if not j_cg_log.the_log:
                return

            # Check for invalid block numbers before processing
            for block_num in j_cg_log.the_log:
                if block_num >= bNum_tConst.NUM_DISK_BLOCKS.value:
                    raise ValueError(f"Invalid block number: {block_num}")

            # Filter out blocks with empty change lists
            blocks = [(blk_num, changes) for blk_num, changes in j_cg_log.the_log.items() if changes]

            if not blocks:
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
            """Coordinate writing buffered pages to disk."""
            pages_to_write = [item for item in self.pg_buf if item is not None]

            if not pages_to_write and not is_end:
                return True

            try:
                for i, (block_num, page) in enumerate(pages_to_write):
                    # Verify CRC
                    if not self._journal.verify_page_crc((block_num, page)):
                        return False

                    # Delegate to Journal for actual write
                    self._journal.write_block_to_disk(block_num, page)

                # Clear the buffer
                self.pg_buf = [None] * self._journal.PAGE_BUFFER_SIZE
                return True
            except Exception as e:
                return False


def test_basic_io():
    """Test basic I/O operations."""
    from status import Status
    from simDisk import SimDisk
    from change import ChangeLog
    from crashChk import CrashChk

    # Setup test files
    test_journal_file = "test_journal.bin"
    test_disk_file = "test_disk.bin"
    test_free_file = "test_free.bin"
    test_inode_file = "test_inode.bin"
    test_status_file = "test_status.txt"

    # Create test instances
    status = Status(test_status_file)
    sim_disk = SimDisk(status, test_disk_file, test_journal_file,
                       test_free_file, test_inode_file)
    change_log = ChangeLog()
    crash_chk = CrashChk()

    try:
        # Create journal instance
        journal = Journal(test_journal_file, sim_disk, change_log,
                          status, crash_chk)

        # Test write and read
        test_data = b"test data"
        journal.seek(journal.META_LEN)
        bytes_written = journal.write(test_data)
        print(f"Wrote {bytes_written} bytes")
        print(f"Total bytes written: {journal.total_bytes_written}")

        journal.seek(journal.META_LEN)
        read_data = journal.read(len(test_data))
        print(f"Read {len(read_data)} bytes: {read_data}")
        print(f"Total bytes read: {journal.total_bytes_read}")

        # Test counter reset
        journal.reset_counters()
        print(f"After reset - Total read: {journal.total_bytes_read}, "
              f"Total written: {journal.total_bytes_written}")

        return True

    except Exception as e:
        print(f"Test failed: {e}")
        return False
    finally:
        # Clean up test files
        import os
        for file in [test_journal_file, test_disk_file, test_free_file,
                     test_inode_file, test_status_file]:
            if os.path.exists(file):
                os.remove(file)


if __name__ == "__main__":
    if test_basic_io():
        print("Basic I/O test passed")
    else:
        print("Basic I/O test failed")

    # Minimal test for journal purging
    from ajUtils import set_test_mode
    from change import Change, ChangeLog
    from ajTypes import u32Const
    from crashChk import CrashChk
    from status import Status
    from simDisk import SimDisk  # Add this import

    set_test_mode(True)

    # Create test files
    test_journal_file = "test_journal.bin"
    test_disk_file = "test_disk.bin"
    test_free_file = "test_free.bin"
    test_inode_file = "test_inode.bin"
    test_status_file = "test_status.txt"

    # Setup proper components
    status = Status(test_status_file)
    sim_disk = SimDisk(status, test_disk_file, test_journal_file,
                       test_free_file, test_inode_file)
    change_log = ChangeLog(test_sw=True)
    crash_chk = CrashChk()

    # Create a test change
    change = Change(1)
    change.add_line(0, b'A' * u32Const.BYTES_PER_LINE.value)
    change_log.add_to_log(change)

    # Create journal with proper SimDisk instance
    test_journal = Journal(test_journal_file, sim_disk, change_log, status, crash_chk)
    test_journal._change_log_handler.calculate_ct_bytes_to_write(change_log)
    test_journal.write_change_log_to_journal(change_log)

    # Test purging
    test_journal.purge_jrnl(True, False)

    print(f"Expected bytes: {test_journal.ct_bytes_to_write + test_journal.META_LEN}")
    print(f"Actual bytes read: {test_journal.total_bytes_read}")

    # Clean up
    import os
    for file in [test_journal_file, test_disk_file, test_free_file,
                 test_inode_file, test_status_file]:
        if os.path.exists(file):
            os.remove(file)