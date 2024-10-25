import pytest
import os
import tempfile
from inodeTable import InodeTable, Inode
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