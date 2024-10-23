import pytest
from myMemory import Memory, Page
from ajTypes import u32Const


@pytest.fixture
def empty_page():
    """Create an empty Page object."""
    return Page()


@pytest.fixture
def empty_memory():
    """Create an empty Memory object."""
    return Memory()


@pytest.fixture
def partially_filled_memory():
    """Create a Memory object with some slots taken."""
    memory = Memory()
    # Take first 5 slots
    for i in range(5):
        slot = memory.get_first_avl_mem_slt()
        assert slot < memory.num_mem_slots
    return memory


class TestPage:
    def test_page_initialization(self, empty_page):
        """Test Page initialization."""
        assert isinstance(empty_page.dat, bytearray)
        assert len(empty_page.dat) == u32Const.BYTES_PER_PAGE.value
        assert all(b == 0 for b in empty_page.dat)  # All bytes should be zero

    def test_page_data_modification(self, empty_page):
        """Test modifying Page data."""
        # Write some test data
        test_data = b'A' * u32Const.BYTES_PER_LINE.value
        start_pos = 0
        end_pos = u32Const.BYTES_PER_LINE.value
        empty_page.dat[start_pos:end_pos] = test_data

        # Verify the data
        assert empty_page.dat[start_pos:end_pos] == test_data
        assert len(empty_page.dat) == u32Const.BYTES_PER_PAGE.value  # Length shouldn't change

    def test_page_data_bounds(self, empty_page):
        """Test Page data bounds checking."""
        with pytest.raises(IndexError):
            empty_page.dat[u32Const.BYTES_PER_PAGE.value] = 0  # Should raise IndexError

    def test_page_data_type_checking(self, empty_page):
        """Test Page data type checking."""
        with pytest.raises(TypeError):
            empty_page.dat[0] = "not a byte"  # Should raise TypeError


class TestMemory:
    def test_memory_initialization(self, empty_memory):
        """Test Memory initialization."""
        assert empty_memory.num_mem_slots == u32Const.NUM_MEM_SLOTS.value
        assert len(empty_memory.the_mem) == u32Const.NUM_MEM_SLOTS.value
        assert len(empty_memory.avl_mem_slts) == u32Const.NUM_MEM_SLOTS.value
        assert all(empty_memory.avl_mem_slts)  # All slots should be available
        assert all(isinstance(page, Page) for page in empty_memory.the_mem)

    def test_get_first_available_slot(self, empty_memory):
        """Test getting the first available memory slot."""
        first_slot = empty_memory.get_first_avl_mem_slt()
        assert first_slot == 0  # First slot should be 0
        assert not empty_memory.avl_mem_slts[0]  # Slot should be marked as unavailable

    def test_get_all_slots(self, empty_memory):
        """Test getting all memory slots."""
        slots = []
        for _ in range(u32Const.NUM_MEM_SLOTS.value):
            slot = empty_memory.get_first_avl_mem_slt()
            assert slot < u32Const.NUM_MEM_SLOTS.value
            slots.append(slot)

        # Next attempt should return NUM_MEM_SLOTS (indicating no slots available)
        assert empty_memory.get_first_avl_mem_slt() == u32Const.NUM_MEM_SLOTS.value

    def test_make_slot_available(self, partially_filled_memory):
        """Test making a memory slot available."""
        # Make slot 0 available
        assert partially_filled_memory.make_avl_mem_slt(0)
        assert partially_filled_memory.avl_mem_slts[0]

        # Try to make an already available slot available
        assert not partially_filled_memory.make_avl_mem_slt(0)

    def test_take_available_slot(self, empty_memory):
        """Test taking an available memory slot."""
        # Take an available slot
        assert empty_memory.take_avl_mem_slt(0)
        assert not empty_memory.avl_mem_slts[0]

        # Try to take an unavailable slot
        assert not empty_memory.take_avl_mem_slt(0)

    def test_get_page(self, empty_memory):
        """Test getting a page from memory."""
        page = empty_memory.get_page(0)
        assert isinstance(page, Page)
        assert len(page.dat) == u32Const.BYTES_PER_PAGE.value

    def test_slot_management_sequence(self, empty_memory):
        """Test a sequence of slot management operations."""
        # Get first slot
        slot1 = empty_memory.get_first_avl_mem_slt()
        assert slot1 == 0
        assert not empty_memory.avl_mem_slts[0]

        # Make it available again
        assert empty_memory.make_avl_mem_slt(slot1)
        assert empty_memory.avl_mem_slts[0]

        # Take it explicitly
        assert empty_memory.take_avl_mem_slt(slot1)
        assert not empty_memory.avl_mem_slts[0]

        # Try to take it again
        assert not empty_memory.take_avl_mem_slt(slot1)

    def test_invalid_slot_operations(self, empty_memory):
        """Test operations with invalid slot numbers."""
        invalid_slot = u32Const.NUM_MEM_SLOTS.value

        # Test make_avl_mem_slt with invalid slot
        with pytest.raises(IndexError):
            empty_memory.make_avl_mem_slt(invalid_slot)

        # Test take_avl_mem_slt with invalid slot
        with pytest.raises(IndexError):
            empty_memory.take_avl_mem_slt(invalid_slot)

        # Test get_page with invalid slot
        with pytest.raises(IndexError):
            empty_memory.get_page(invalid_slot)

    def test_memory_slot_independence(self, empty_memory):
        """Test that memory slots are independent."""
        # Get two slots
        slot1 = empty_memory.get_first_avl_mem_slt()
        slot2 = empty_memory.get_first_avl_mem_slt()

        # Modify data in first slot
        page1 = empty_memory.get_page(slot1)
        page1.dat[0] = 0xFF

        # Verify second slot is unaffected
        page2 = empty_memory.get_page(slot2)
        assert page2.dat[0] == 0