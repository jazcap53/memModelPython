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


class TestInodeTable:
    """Tests for the (refactored) InodeTable implementation."""

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

    def test_full_table(self, inode_table):
        """Test behavior when inode table is full."""
        # Fill the table
        inodes = []
        while True:
            inode_num = inode_table.create_inode()
            if inode_num == SENTINEL_INUM:
                break
            inodes.append(inode_num)

        # Verify table is full
        assert inode_table.create_inode() == SENTINEL_INUM
        assert len(inodes) == u32Const.NUM_INODE_TBL_BLOCKS.value * lNum_tConst.INODES_PER_BLOCK.value

    def test_concurrent_block_assignment(self, inode_table):
        """Test assigning blocks to multiple inodes simultaneously."""
        # Create multiple inodes
        inode_nums = [inode_table.create_inode() for _ in range(3)]

        # Assign blocks to each inode
        for i, inode_num in enumerate(inode_nums):
            for j in range(2):
                block_num = i * 2 + j
                assert inode_table.assign_block(inode_num, block_num)

        # Verify assignments
        for i, inode_num in enumerate(inode_nums):
            inode = inode_table.storage.get_inode(inode_num)
            blocks = [b for b in inode.b_nums if b != SENTINEL_BNUM]
            assert blocks == [i * 2, i * 2 + 1]

    @pytest.mark.xfail(reason="Indirect block handling not implemented")
    def test_indirect_block_operations(self, inode_table):
        """Test operations with indirect blocks."""
        inode_num = inode_table.create_inode()

        # Fill direct blocks
        for i in range(u32Const.CT_INODE_BNUMS.value):
            assert inode_table.assign_block(inode_num, i)

        # Try to assign more blocks (should use indirect blocks if implemented)
        additional_blocks = min(u32Const.CT_INODE_INDIRECTS.value, 3)  # Test with a few indirect blocks
        for i in range(additional_blocks):
            block_num = 100 + i
            assert inode_table.assign_block(inode_num, block_num), f"Failed to assign indirect block {block_num}"

        # Verify indirect blocks
        inode = inode_table.storage.get_inode(inode_num)
        for i in range(additional_blocks):
            block_num = 100 + i
            assert block_num in inode.indirect, f"Indirect block {block_num} not found after assignment"

    def test_load_nonexistent_file(self, temp_inode_file):
        """Test loading from a non-existent file."""
        # Ensure the file doesn't exist
        if os.path.exists(temp_inode_file):
            os.unlink(temp_inode_file)

        table = InodeTable(temp_inode_file)
        # Should not raise exceptions, but start fresh
        table.load()

        # Verify table is in initial state
        assert table.allocator.avail.all()
        assert not table.storage.modified

    def test_store_with_permission_error(self, temp_inode_file):
        """Test storing with insufficient permissions."""
        table = InodeTable(temp_inode_file)

        # Create an inode to make the table modified
        table.create_inode()

        # Make the file read-only
        os.chmod(temp_inode_file, 0o444)

        # Should handle the permission error gracefully
        with pytest.raises(PermissionError):
            table.store()

        # Cleanup
        os.chmod(temp_inode_file, 0o666)


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

    def test_block_management_edge_cases(self):
        """Test edge cases in InodeBlockManager."""
        manager = InodeBlockManager()
        inode = Inode()

        # Test with sentinel block number
        assert not manager.assign_block(inode, SENTINEL_BNUM)
        assert not manager.release_block(inode, SENTINEL_BNUM)

        # Test releasing non-existent block
        assert not manager.release_block(inode, 999)

        # Test assigning blocks when full
        for i in range(u32Const.CT_INODE_BNUMS.value):
            assert manager.assign_block(inode, i)

        # Try to assign one more block
        assert not manager.assign_block(inode, u32Const.CT_INODE_BNUMS.value)

    @pytest.mark.xfail(reason="Indirect block handling not implemented")
    def test_indirect_block_operations(self):
        """Test indirect block assignment and release."""
        manager = InodeBlockManager()
        inode = Inode()

        # Fill direct blocks
        for i in range(u32Const.CT_INODE_BNUMS.value):
            assert manager.assign_block(inode, i)

        # Try to assign an indirect block
        assert manager.assign_block(inode, 100), "Failed to assign indirect block"
        assert 100 in inode.indirect, "Indirect block not found after assignment"


class TestInodeAllocatorBoundaries:
    """Tests for boundary conditions in InodeAllocator."""

    def test_allocate_full_table(self):
        """Test allocation when table is full."""
        # Create a small allocator for easier testing
        allocator = InodeAllocator(2, 4)  # 8 total inodes

        # Allocate all inodes
        inodes = [allocator.allocate() for _ in range(8)]
        assert all(i is not None for i in inodes)

        # Try to allocate one more
        assert allocator.allocate() is None

    def test_deallocate_edge_cases(self):
        """Test deallocation edge cases."""
        allocator = InodeAllocator(2, 4)

        # Deallocate unallocated inode
        allocator.deallocate(0)  # Should not raise error

        # Deallocate sentinel value
        allocator.deallocate(SENTINEL_INUM)  # Should not raise error

        # Deallocate out of range inode
        allocator.deallocate(100)  # Should not raise error or corrupt state

    def test_is_available_edge_cases(self):
        """Test is_available edge cases."""
        allocator = InodeAllocator(2, 4)

        # Test with valid unallocated inode
        assert allocator.is_available(0) == True

        # Test with sentinel value
        assert allocator.is_available(SENTINEL_INUM) == False

        # Test with out of range inode
        assert allocator.is_available(100) == False


class TestInodeStorageInternals:
    """Tests for internal methods of InodeStorage."""

    @pytest.fixture
    def storage_with_data(self, temp_inode_file):
        """Create storage with some test data."""
        storage = InodeStorage(temp_inode_file)
        inode = storage.get_inode(0)
        inode.b_nums[0] = 42
        inode.lkd = 1
        inode.cr_time = 1000
        inode.indirect[0] = 99
        storage.modified = True
        return storage

    def test_read_write_inode(self, storage_with_data, temp_inode_file):
        """Test _read_inode and _write_inode methods."""
        storage = storage_with_data

        # Store the data
        storage.store_table()

        # Create new storage to read data
        new_storage = InodeStorage(temp_inode_file)
        new_storage.load_table()

        # Verify data was read correctly
        inode = new_storage.get_inode(0)
        assert inode.b_nums[0] == 42
        assert inode.lkd == 1
        assert inode.cr_time == 1000
        assert inode.indirect[0] == 99


