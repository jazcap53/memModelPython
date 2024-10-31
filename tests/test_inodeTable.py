import pytest
import os
import tempfile
from inodeTable import (Inode, InodeStorage, InodeAllocator,
                       InodeBlockManager, InodeTable)
from ajTypes import u32Const, lNum_tConst, SENTINEL_INUM, SENTINEL_BNUM



@pytest.fixture
def temp_inode_file():
    """Create a temporary file for inode table testing."""
    with tempfile.NamedTemporaryFile(delete=False, mode='wb') as tf:
        # Initialize the file with required structure

        # Write availability bitmap (all ones = all inodes available)
        avail_bytes = b'\xff' * ((u32Const.NUM_INODE_TBL_BLOCKS.value *
                                  lNum_tConst.INODES_PER_BLOCK.value + 7) // 8)
        tf.write(avail_bytes)

        # Write inode table entries
        for i in range(u32Const.NUM_INODE_TBL_BLOCKS.value):
            for j in range(lNum_tConst.INODES_PER_BLOCK.value):
                # Write block numbers (all SENTINEL)
                tf.write(SENTINEL_BNUM.to_bytes(4, 'little') * u32Const.CT_INODE_BNUMS.value)
                # Write locked status
                tf.write(SENTINEL_INUM.to_bytes(4, 'little'))
                # Write creation time
                tf.write((0).to_bytes(8, 'little'))
                # Write indirect blocks
                tf.write(SENTINEL_BNUM.to_bytes(4, 'little') * u32Const.CT_INODE_INDIRECTS.value)
                # Write inode number
                tf.write((i * lNum_tConst.INODES_PER_BLOCK.value + j).to_bytes(4, 'little'))

        filename = tf.name

    yield filename

    # Cleanup after tests
    if os.path.exists(filename):
        os.unlink(filename)


@pytest.fixture
def empty_inode_table(temp_inode_file):
    """Create a fresh InodeTable instance."""
    return InodeTable(temp_inode_file)


@pytest.fixture
def inode_with_blocks():
    """Create an Inode with some test block numbers."""
    inode = Inode()
    inode.b_nums = [1, 2, 3] + [SENTINEL_BNUM] * (u32Const.CT_INODE_BNUMS.value - 3)
    inode.cr_time = 1000
    inode.i_num = 1
    return inode


class TestInode:
    """Test the Inode class."""

    def test_inode_initialization(self):
        """Test default Inode initialization."""
        inode = Inode()

        # Check default values
        assert len(inode.b_nums) == u32Const.CT_INODE_BNUMS.value
        assert all(bn == SENTINEL_BNUM for bn in inode.b_nums)
        assert inode.lkd == SENTINEL_INUM
        assert inode.cr_time == 0
        assert len(inode.indirect) == u32Const.CT_INODE_INDIRECTS.value
        assert all(ind == SENTINEL_BNUM for ind in inode.indirect)
        assert inode.i_num == SENTINEL_INUM

    def test_inode_block_assignment(self):
        """Test assigning block numbers to an Inode."""
        inode = Inode()
        test_blocks = [1, 2, 3]

        for i, block in enumerate(test_blocks):
            inode.b_nums[i] = block

        assert inode.b_nums[:3] == test_blocks
        assert all(bn == SENTINEL_BNUM for bn in inode.b_nums[3:])


@pytest.mark.skip(reason="Tests not yet compatible with refactored code")
class TestInodeTable:
    """Test the InodeTable class."""

    def test_table_initialization(self, empty_inode_table):
        """Test InodeTable initialization."""
        # Check initial state
        assert empty_inode_table.modified == False
        assert empty_inode_table.avail.all()  # All inodes should be available

        # Check table dimensions
        assert len(empty_inode_table.tbl) == u32Const.NUM_INODE_TBL_BLOCKS.value
        for block in empty_inode_table.tbl:
            assert len(block) == lNum_tConst.INODES_PER_BLOCK.value

    def test_assign_inode(self, empty_inode_table):
        """Test assigning (allocating) an inode."""
        inode_num = empty_inode_table.assign_in_n()

        # Check assignment
        assert inode_num != SENTINEL_INUM
        assert inode_num >= 0
        assert not empty_inode_table.avail.test(inode_num)  # Should be marked as in use
        assert empty_inode_table.modified == True

        # Check the assigned inode's initial state
        inode = empty_inode_table.ref_tbl_node(inode_num)
        assert inode.cr_time > 0
        assert all(bn == SENTINEL_BNUM for bn in inode.b_nums)

    def test_release_inode(self, empty_inode_table):
        """Test releasing (deallocating) an inode."""
        # First assign an inode
        inode_num = empty_inode_table.assign_in_n()

        # Then release it
        empty_inode_table.release_in_n(inode_num)

        # Check release
        assert empty_inode_table.avail.test(inode_num)  # Should be marked as available
        inode = empty_inode_table.ref_tbl_node(inode_num)
        assert all(bn == SENTINEL_BNUM for bn in inode.b_nums)
        assert inode.cr_time == 0

    def test_node_in_use(self, empty_inode_table):
        """Test checking if an inode is in use."""
        # Check sentinel value
        assert not empty_inode_table.node_in_use(SENTINEL_INUM)

        # Check unused inode
        assert not empty_inode_table.node_in_use(0)

        # Check assigned inode
        inode_num = empty_inode_table.assign_in_n()
        assert empty_inode_table.node_in_use(inode_num)

    def test_node_locked(self, empty_inode_table):
        """Test inode locking functionality."""
        # Assign an inode
        inode_num = empty_inode_table.assign_in_n()
        inode = empty_inode_table.ref_tbl_node(inode_num)

        # Initially should be unlocked
        assert not empty_inode_table.node_locked(inode_num)

        # Lock it
        inode.lkd = 1
        assert empty_inode_table.node_locked(inode_num)

        # Unlock it
        inode.lkd = SENTINEL_INUM
        assert not empty_inode_table.node_locked(inode_num)

    def test_assign_block_numbers(self, empty_inode_table):
        """Test assigning block numbers to an inode."""
        inode_num = empty_inode_table.assign_in_n()

        # Assign some blocks
        assert empty_inode_table.assign_blk_n(inode_num, 1)
        assert empty_inode_table.assign_blk_n(inode_num, 2)

        # Verify assignments
        inode = empty_inode_table.ref_tbl_node(inode_num)
        assert 1 in inode.b_nums
        assert 2 in inode.b_nums

    def test_release_block_numbers(self, empty_inode_table):
        """Test releasing block numbers from an inode."""
        # First assign an inode and some blocks
        inode_num = empty_inode_table.assign_in_n()
        empty_inode_table.assign_blk_n(inode_num, 1)
        empty_inode_table.assign_blk_n(inode_num, 2)

        # Release one block
        assert empty_inode_table.release_blk_n(inode_num, 1)

        # Verify the release
        inode = empty_inode_table.ref_tbl_node(inode_num)
        assert 1 not in inode.b_nums
        assert 2 in inode.b_nums

        # Try to release non-existent block
        assert not empty_inode_table.release_blk_n(inode_num, 999)

    def test_release_all_blocks(self, empty_inode_table):
        """Test releasing all blocks from an inode."""
        # Assign inode and blocks
        inode_num = empty_inode_table.assign_in_n()
        empty_inode_table.assign_blk_n(inode_num, 1)
        empty_inode_table.assign_blk_n(inode_num, 2)

        # Release all blocks
        empty_inode_table.release_all_blk_n(inode_num)

        # Verify all blocks are released
        inode = empty_inode_table.ref_tbl_node(inode_num)
        assert all(bn == SENTINEL_BNUM for bn in inode.b_nums)

    def test_list_all_blocks(self, empty_inode_table):
        """Test listing all blocks associated with an inode."""
        # Test with sentinel inode number
        assert empty_inode_table.list_all_blk_n(SENTINEL_INUM) == []

        # Test with real inode
        inode_num = empty_inode_table.assign_in_n()
        empty_inode_table.assign_blk_n(inode_num, 1)
        empty_inode_table.assign_blk_n(inode_num, 2)

        block_list = empty_inode_table.list_all_blk_n(inode_num)
        assert block_list == [1, 2]

    def test_store_and_load_table(self, empty_inode_table, temp_inode_file):
        """Test storing and loading the inode table."""
        # Assign some inodes and blocks
        inode_num1 = empty_inode_table.assign_in_n()
        empty_inode_table.assign_blk_n(inode_num1, 1)

        inode_num2 = empty_inode_table.assign_in_n()
        empty_inode_table.assign_blk_n(inode_num2, 2)

        # Verify initial state
        print(f"\nInitial state:")
        print(f"inode_num1: {inode_num1}, in_use: {empty_inode_table.node_in_use(inode_num1)}")
        print(f"inode_num2: {inode_num2}, in_use: {empty_inode_table.node_in_use(inode_num2)}")
        print(f"Availability bitmap: {empty_inode_table.avail.to_bytes().hex()}")

        # Store the table
        empty_inode_table.store_tbl()

        # Verify file contents after store
        with open(temp_inode_file, 'rb') as f:
            avail_bytes = f.read((u32Const.NUM_INODE_TBL_BLOCKS.value *
                                  lNum_tConst.INODES_PER_BLOCK.value + 7) // 8)
            print(f"\nStored availability bitmap: {avail_bytes.hex()}")

        # Create new table instance and load from file
        new_table = InodeTable(temp_inode_file)

        # Debug loaded state
        print(f"\nLoaded state:")
        print(f"inode_num1: {inode_num1}, in_use: {new_table.node_in_use(inode_num1)}")
        print(f"inode_num2: {inode_num2}, in_use: {new_table.node_in_use(inode_num2)}")
        print(f"Loaded availability bitmap: {new_table.avail.to_bytes().hex()}")

        # Original assertions
        assert new_table.node_in_use(inode_num1)
        assert new_table.node_in_use(inode_num2)
        assert 1 in new_table.ref_tbl_node(inode_num1).b_nums
        assert 2 in new_table.ref_tbl_node(inode_num2).b_nums

        # Additional verification
        assert new_table.avail.to_bytes() == empty_inode_table.avail.to_bytes()

    def test_full_table(self, empty_inode_table):
        """Test behavior when inode table is full."""
        # Fill the table
        inodes = []
        while True:
            inode_num = empty_inode_table.assign_in_n()
            if inode_num == SENTINEL_INUM:
                break
            inodes.append(inode_num)

        # Verify table is full
        assert empty_inode_table.assign_in_n() == SENTINEL_INUM
        assert len(inodes) == u32Const.NUM_INODE_TBL_BLOCKS.value * lNum_tConst.INODES_PER_BLOCK.value

    def test_invalid_inode_operations(self, empty_inode_table):
        """Test operations with invalid inode numbers."""
        # Test operations with SENTINEL_INUM
        assert not empty_inode_table.node_in_use(SENTINEL_INUM)
        assert empty_inode_table.list_all_blk_n(SENTINEL_INUM) == []

        # Test releasing non-existent block from valid inode
        inode_num = empty_inode_table.assign_in_n()
        assert not empty_inode_table.release_blk_n(inode_num, 999)

    def test_ensure_stored_behavior(self, empty_inode_table):
        """Test the ensure_stored method."""
        # Initially not modified
        assert not empty_inode_table.modified
        empty_inode_table.ensure_stored()

        # Modify table
        inode_num = empty_inode_table.assign_in_n()
        assert empty_inode_table.modified

        # Store and verify not modified
        empty_inode_table.ensure_stored()
        assert not empty_inode_table.modified

    def test_concurrent_block_assignment(self, empty_inode_table):
        """Test assigning blocks to multiple inodes."""
        # Assign multiple inodes
        inode_nums = [empty_inode_table.assign_in_n() for _ in range(3)]

        # Assign blocks to each inode
        for i, inode_num in enumerate(inode_nums):
            for j in range(2):
                block_num = i * 2 + j
                assert empty_inode_table.assign_blk_n(inode_num, block_num)

        # Verify assignments
        for i, inode_num in enumerate(inode_nums):
            blocks = empty_inode_table.list_all_blk_n(inode_num)
            assert blocks == [i * 2, i * 2 + 1]

    def test_inode_reuse(self, empty_inode_table):
        """Test reusing released inodes."""
        # Assign and release an inode
        inode_num = empty_inode_table.assign_in_n()
        empty_inode_table.assign_blk_n(inode_num, 1)
        empty_inode_table.release_in_n(inode_num)

        # Reassign the same inode number
        new_inode_num = empty_inode_table.assign_in_n()
        assert new_inode_num == inode_num

        # Verify it's clean
        inode = empty_inode_table.ref_tbl_node(new_inode_num)
        assert all(bn == SENTINEL_BNUM for bn in inode.b_nums)
        assert inode.cr_time > 0


class TestRefactoredInodeTable:
    """Tests for the refactored InodeTable implementation."""

    @pytest.fixture
    def inode_table(self, temp_inode_file):
        """Create a fresh InodeTable instance."""
        table = InodeTable(temp_inode_file)
        table.load()  # Ensure table is loaded
        return table

    def test_table_initialization(self, inode_table):
        """Test InodeTable initialization."""
        assert not inode_table.storage.modified
        assert inode_table.allocator.avail.all()  # All inodes should be available
        assert isinstance(inode_table.block_manager, InodeBlockManager)
        assert isinstance(inode_table.storage, InodeStorage)

    def test_create_and_delete_inode(self, inode_table):
        """Test creating and deleting an inode."""
        # Create inode
        inode_num = inode_table.create_inode()
        assert inode_num != SENTINEL_INUM
        assert inode_table.storage.modified
        assert inode_table.is_in_use(inode_num)

        # Check the created inode's properties
        inode = inode_table.storage.get_inode(inode_num)
        assert inode.cr_time > 0
        assert all(bn == SENTINEL_BNUM for bn in inode.b_nums)
        assert inode.lkd == SENTINEL_INUM

        # Delete inode
        inode_table.delete_inode(inode_num)
        assert not inode_table.is_in_use(inode_num)

        # Check the deleted inode's properties
        inode = inode_table.storage.get_inode(inode_num)
        assert inode.cr_time == 0
        assert all(bn == SENTINEL_BNUM for bn in inode.b_nums)
        assert inode.lkd == SENTINEL_INUM

    def test_block_operations(self, inode_table):
        """Test block assignment and release."""
        inode_num = inode_table.create_inode()

        # Assign block
        assert inode_table.assign_block(inode_num, 1)
        inode = inode_table.storage.get_inode(inode_num)
        assert 1 in inode.b_nums
        assert inode_table.storage.modified

        # Release block
        assert inode_table.release_block(inode_num, 1)
        assert 1 not in inode.b_nums

        # Try to assign too many blocks
        for i in range(u32Const.CT_INODE_BNUMS.value):
            inode_table.assign_block(inode_num, i)
        # This one should fail as inode is full
        assert not inode_table.assign_block(inode_num, u32Const.CT_INODE_BNUMS.value)

    def test_inode_locking(self, inode_table):
        """Test inode locking status."""
        inode_num = inode_table.create_inode()
        assert not inode_table.is_locked(inode_num)

        # Lock inode
        inode = inode_table.storage.get_inode(inode_num)
        inode.lkd = 1
        assert inode_table.is_locked(inode_num)

        # Unlock inode
        inode.lkd = SENTINEL_INUM
        assert not inode_table.is_locked(inode_num)

    def test_store_and_load(self, inode_table):
        """Test storing and loading the inode table."""
        # Create and modify some inodes
        inode_num1 = inode_table.create_inode()
        inode_table.assign_block(inode_num1, 1)

        # Store table
        inode_table.store()
        assert not inode_table.storage.modified

        # Create new table instance and verify state
        new_table = InodeTable(inode_table.storage.filename)
        new_table.load()  # Explicitly load the table
        assert new_table.is_in_use(inode_num1)
        inode = new_table.storage.get_inode(inode_num1)
        assert 1 in inode.b_nums

    def test_sentinel_operations(self, inode_table):
        """Test operations with sentinel values."""
        # Test operations with SENTINEL_INUM
        assert not inode_table.is_in_use(SENTINEL_INUM)
        assert not inode_table.is_locked(SENTINEL_INUM)

        # Deleting SENTINEL_INUM should not raise errors
        inode_table.delete_inode(SENTINEL_INUM)

    def test_multiple_inodes(self, inode_table):
        """Test operations with multiple inodes."""
        # Create multiple inodes
        inode_nums = []
        for i in range(3):
            inode_num = inode_table.create_inode()
            assert inode_num != SENTINEL_INUM
            inode_nums.append(inode_num)

        # Verify each inode is independent
        for i, inode_num in enumerate(inode_nums):
            assert inode_table.is_in_use(inode_num)
            assert inode_table.assign_block(inode_num, i)
            inode = inode_table.storage.get_inode(inode_num)
            assert i in inode.b_nums

        # Delete middle inode
        inode_table.delete_inode(inode_nums[1])
        assert not inode_table.is_in_use(inode_nums[1])
        assert inode_table.is_in_use(inode_nums[0])
        assert inode_table.is_in_use(inode_nums[2])

    def test_inode_reuse(self, inode_table):
        """Test that deleted inodes can be reused."""
        # Create and delete an inode
        inode_num1 = inode_table.create_inode()
        inode_table.assign_block(inode_num1, 1)
        inode_table.delete_inode(inode_num1)

        # Create new inode - should get the same number
        inode_num2 = inode_table.create_inode()
        assert inode_num2 == inode_num1

        # Verify it's a clean inode
        inode = inode_table.storage.get_inode(inode_num2)
        assert all(bn == SENTINEL_BNUM for bn in inode.b_nums)
        assert inode.cr_time > 0
        assert inode.lkd == SENTINEL_INUM

    def test_error_conditions(self, inode_table):
        """Test error conditions and edge cases."""
        # Try to assign/release blocks on non-existent inode
        assert not inode_table.assign_block(SENTINEL_INUM, 1)
        assert not inode_table.release_block(SENTINEL_INUM, 1)

        # Create inode and try invalid block operations
        inode_num = inode_table.create_inode()
        assert not inode_table.assign_block(inode_num, SENTINEL_BNUM)  # Add this line
        assert not inode_table.release_block(inode_num, SENTINEL_BNUM)

        # Try to release non-existent block
        assert not inode_table.release_block(inode_num, 999)


class TestInodeStorage:
    """Tests for the InodeStorage class."""

    @pytest.fixture
    def storage(self, temp_inode_file):
        return InodeStorage(temp_inode_file)

    def test_get_inode(self, storage):
        """Test retrieving an inode."""
        inode = storage.get_inode(0)
        assert isinstance(inode, Inode)
        assert inode.b_nums == [SENTINEL_BNUM] * u32Const.CT_INODE_BNUMS.value


class TestInodeAllocator:
    """Tests for the InodeAllocator class."""

    @pytest.fixture
    def allocator(self):
        return InodeAllocator(u32Const.NUM_INODE_TBL_BLOCKS.value,
                            lNum_tConst.INODES_PER_BLOCK.value)

    def test_allocate_deallocate(self, allocator):
        """Test allocation and deallocation."""
        inode_num = allocator.allocate()
        assert inode_num is not None
        assert not allocator.is_available(inode_num)

        allocator.deallocate(inode_num)
        assert allocator.is_available(inode_num)


class TestInodeBlockManager:
    """Tests for the InodeBlockManager class."""

    def test_block_operations(self):
        """Test block assignment and release."""
        manager = InodeBlockManager()
        inode = Inode()

        assert manager.assign_block(inode, 1)
        assert 1 in manager.list_blocks(inode)

        assert manager.release_block(inode, 1)
        assert 1 not in manager.list_blocks(inode)