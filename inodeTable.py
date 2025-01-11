"""
inodeTable.py: Manages inode allocation and block number assignments

This module provides classes for managing inodes in the file system:
- Inode: Data structure representing a single inode
- InodeStorage: Handles low-level storage operations
- InodeAllocator: Manages inode allocation and deallocation
- InodeBlockManager: Handles block number assignments
- InodeTable: Main class coordinating the above components
"""
__all__ = ['Inode', 'InodeStorage', 'InodeAllocator', 'InodeBlockManager', 'InodeTable']


from dataclasses import dataclass
import struct
from typing import List, Optional, Set, BinaryIO
import logging
from ajTypes import bNum_t, inNum_t, u32Const, lNum_tConst, SENTINEL_INUM, SENTINEL_BNUM
from ajUtils import get_cur_time, Tabber
from fileShifter import FileShifter
from arrBit import ArrBit


# Setup module logger
logger = logging.getLogger(__name__)


@dataclass
class Inode:
    """Represents a single inode in the file system."""
    b_nums: List[bNum_t]  # Direct block numbers
    lkd: inNum_t  # Lock status
    cr_time: int  # Creation timestamp
    indirect: List[bNum_t]  # Indirect block numbers
    i_num: inNum_t  # Inode number

    def __init__(self):
        self.b_nums = [SENTINEL_BNUM] * u32Const.CT_INODE_BNUMS.value
        self.lkd = SENTINEL_INUM
        self.cr_time = 0
        self.indirect = [SENTINEL_BNUM] * u32Const.CT_INODE_INDIRECTS.value
        self.i_num = SENTINEL_INUM

    def is_locked(self) -> bool:
        """Check if this inode is locked."""
        return self.lkd != SENTINEL_INUM

    def clear(self) -> None:
        """Reset this inode to its initial state."""
        self.__init__()


class InodeStorage:
    """Handles storage and retrieval of inode data."""

    def __init__(self, filename: str):
        self.filename = filename
        self.shifter = FileShifter()
        self.modified = False
        self.tbl = self._create_empty_table()

    def _create_empty_table(self) -> List[List[Inode]]:
        """Create an empty two-dimensional table of inodes."""
        return [[Inode() for _ in range(lNum_tConst.INODES_PER_BLOCK.value)]
                for _ in range(u32Const.NUM_INODE_TBL_BLOCKS.value)]

    def get_inode(self, inode_num: inNum_t) -> Optional[Inode]:
        """
        Get reference to an inode by its number.

        Returns None if inode_num is invalid or out of range.
        """
        if inode_num == SENTINEL_INUM:
            logger.debug(f"get_inode: Requested sentinel inode number {SENTINEL_INUM}")
            return None

        blk_num = inode_num // lNum_tConst.INODES_PER_BLOCK.value
        blk_ix = inode_num % lNum_tConst.INODES_PER_BLOCK.value

        if (blk_num >= len(self.tbl) or
                blk_ix >= lNum_tConst.INODES_PER_BLOCK.value):
            logger.warning(f"get_inode: Requested inode number {inode_num} is out of range")
            return None

        return self.tbl[blk_num][blk_ix]

    def load_table(self) -> None:
        """Load the inode table from disk."""
        try:
            with open(self.filename, 'rb') as f:
                # Skip availability bitmap (handled by InodeAllocator)
                bitmap_size = (u32Const.NUM_INODE_TBL_BLOCKS.value *
                               lNum_tConst.INODES_PER_BLOCK.value + 7) // 8
                f.seek(bitmap_size)

                # Read inode table entries
                for i in range(u32Const.NUM_INODE_TBL_BLOCKS.value):
                    for j in range(lNum_tConst.INODES_PER_BLOCK.value):
                        self._read_inode(f, i, j)
            logger.info(f"Successfully loaded inode table from {self.filename}")
        except FileNotFoundError:
            logger.warning(f"No inode table file found at {self.filename}. Starting fresh.")
        except Exception as e:
            logger.error(f"Error loading inode table: {str(e)}")
            # Re-raise to ensure caller knows about the failure
            raise

    def store_table(self) -> None:
        """Store the inode table to disk."""
        if not self.modified:
            logger.debug("store_table: No modifications to store")
            return

        def write_table(f):
            # Skip availability bitmap (handled by InodeAllocator)
            bitmap_size = (u32Const.NUM_INODE_TBL_BLOCKS.value *
                           lNum_tConst.INODES_PER_BLOCK.value + 7) // 8
            f.seek(bitmap_size)

            # Write inode table entries
            for block in self.tbl:
                for inode in block:
                    self._write_inode(f, inode)

        try:
            self.shifter.shift_files(self.filename, write_table, binary_mode=True)
            self.modified = False
            logger.info(f"Successfully stored inode table to {self.filename}")
        except Exception as e:
            logger.error(f"Error storing inode table: {str(e)}")
            raise


class InodeAllocator:
    """Manages inode allocation and deallocation."""

    def __init__(self, num_blocks: int, inodes_per_block: int):
        self.avail = ArrBit(num_blocks, inodes_per_block)
        self.avail.set()  # All inodes initially available

    def allocate(self) -> Optional[inNum_t]:
        """
        Allocate a new inode.

        Returns:
            inode number if successful, None if no inodes available
        """
        max_inum = self.avail.total_bits
        for ix in range(max_inum):
            if self.avail.test(ix):
                self.avail.reset(ix)
                return ix
        logger.warning("Failed to allocate inode: all inodes are in use")
        return None

    def deallocate(self, inode_num: inNum_t) -> None:
        """Mark an inode as available."""
        if inode_num == SENTINEL_INUM:
            return
        if 0 <= inode_num < self.avail.total_bits:
            self.avail.set(inode_num)
        else:
            logger.warning(f"Attempted to deallocate out-of-range inode {inode_num}")

    def is_available(self, inode_num: inNum_t) -> bool:
        """Check if an inode is available."""
        if inode_num == SENTINEL_INUM:
            return False
        if 0 <= inode_num < self.avail.total_bits:
            return self.avail.test(inode_num)
        logger.warning(f"Checked availability of out-of-range inode {inode_num}")
        return False


class InodeBlockManager:
    """Handles block number assignments for inodes."""

    @staticmethod
    def assign_block(inode: Inode, block_num: bNum_t) -> bool:
        """
        Assign a block number to an inode.

        Returns:
            True if successful, False if inode is full or block_num is invalid
        """
        if block_num == SENTINEL_BNUM:
            logger.warning("Attempted to assign sentinel block number")
            return False

        for i, item in enumerate(inode.b_nums):
            if item == SENTINEL_BNUM:
                inode.b_nums[i] = block_num
                return True
        logger.warning(f"Failed to assign block {block_num}: inode is full")
        return False

    @staticmethod
    def release_block(inode: Inode, target: bNum_t) -> bool:
        """
        Release a block number from an inode.

        Returns:
            True if block was found and released, False otherwise
        """
        if target == SENTINEL_BNUM:
            logger.warning("Attempted to release sentinel block number")
            return False

        for i, item in enumerate(inode.b_nums):
            if item == target:
                inode.b_nums[i] = SENTINEL_BNUM
                return True
        logger.warning(f"Failed to release block {target}: block not found in inode")
        return False


class InodeTable:
    """Main class coordinating inode operations."""

    def __init__(self, filename: str):
        self.storage = InodeStorage(filename)
        self.allocator = InodeAllocator(u32Const.NUM_INODE_TBL_BLOCKS.value,
                                      lNum_tConst.INODES_PER_BLOCK.value)
        self.block_manager = InodeBlockManager()
        self.tabs = Tabber()

    def create_inode(self) -> inNum_t:
        """Create a new inode."""
        inode_num = self.allocator.allocate()
        if inode_num is not None:
            inode = self.storage.get_inode(inode_num)
            inode.cr_time = get_cur_time(True)
            self.storage.modified = True
            logger.debug(f"Created new inode with number {inode_num}")
            return inode_num
        logger.warning("Failed to create new inode: no available inode numbers")
        return SENTINEL_INUM

    def delete_inode(self, inode_num: inNum_t) -> None:
        """Delete an inode."""
        if inode_num == SENTINEL_INUM:
            logger.warning("Attempted to delete sentinel inode number")
            return
        inode = self.storage.get_inode(inode_num)
        if inode:
            inode.clear()
            self.allocator.deallocate(inode_num)
            self.storage.modified = True
            logger.debug(f"Deleted inode {inode_num}")
        else:
            logger.warning(f"Attempted to delete non-existent inode {inode_num}")

    def assign_block(self, inode_num: inNum_t, block_num: bNum_t) -> bool:
        """Assign a block to an inode."""
        if inode_num == SENTINEL_INUM or block_num == SENTINEL_BNUM:
            logger.warning(f"Attempted to assign block with invalid parameters: inode={inode_num}, block={block_num}")
            return False
        inode = self.storage.get_inode(inode_num)
        if not inode:
            logger.warning(f"Failed to assign block {block_num}: inode {inode_num} not found")
            return False
        success = self.block_manager.assign_block(inode, block_num)
        if success:
            self.storage.modified = True
            logger.debug(f"Assigned block {block_num} to inode {inode_num}")
        return success

    def release_block(self, inode_num: inNum_t, block_num: bNum_t) -> bool:
        """Release a block from an inode."""
        if inode_num == SENTINEL_INUM or block_num == SENTINEL_BNUM:
            logger.warning(f"Attempted to release block with invalid parameters: inode={inode_num}, block={block_num}")
            return False
        inode = self.storage.get_inode(inode_num)
        if not inode:
            logger.warning(f"Failed to release block {block_num}: inode {inode_num} not found")
            return False
        success = self.block_manager.release_block(inode, block_num)
        if success:
            self.storage.modified = True
            logger.debug(f"Released block {block_num} from inode {inode_num}")
        return success

    def is_locked(self, inode_num: inNum_t) -> bool:
        """Check if an inode is locked."""
        if inode_num == SENTINEL_INUM:
            return False
        inode = self.storage.get_inode(inode_num)
        return bool(inode and inode.is_locked())

    def is_in_use(self, inode_num: inNum_t) -> bool:
        """Check if an inode is in use."""
        if inode_num == SENTINEL_INUM:
            return False
        return not self.allocator.is_available(inode_num)

    def store(self) -> None:
        """Store the inode table to disk."""
        try:
            # First write the availability bitmap
            with open(self.storage.filename, 'r+b') as f:
                bitmap_size = (u32Const.NUM_INODE_TBL_BLOCKS.value *
                            lNum_tConst.INODES_PER_BLOCK.value + 7) // 8
                f.write(self.allocator.avail.to_bytes())

            # Then store the inode data
            self.storage.store_table()
        except Exception as e:
            logger.error(f"Error storing inode table: {str(e)}")
            raise

    def load(self) -> None:
        """Load the inode table from disk."""
        try:
            # First read the availability bitmap
            try:
                with open(self.storage.filename, 'rb') as f:
                    bitmap_size = (u32Const.NUM_INODE_TBL_BLOCKS.value *
                                lNum_tConst.INODES_PER_BLOCK.value + 7) // 8
                    bitmap_data = f.read(bitmap_size)
                    self.allocator.avail = ArrBit.from_bytes(
                        bitmap_data,
                        u32Const.NUM_INODE_TBL_BLOCKS.value,
                        lNum_tConst.INODES_PER_BLOCK.value
                    )
            except FileNotFoundError:
                logger.warning(f"No inode table file found at {self.storage.filename}. Starting fresh.")
                return

            # Then load the inode data
            self.storage.load_table()
            logger.info("Successfully loaded inode table")
        except Exception as e:
            logger.error(f"Error loading inode table: {str(e)}")
            raise

    def ensure_stored(self) -> None:
        """Ensure the inode table is stored if modified."""
        if self.storage.modified:
            self.store()