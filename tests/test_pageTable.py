# tests/test_pageTable.py

import pytest
from pageTable import PageTable, PgTabEntry
from ajTypes import u32Const
from typing import List


@pytest.fixture
def empty_page_table():
    """Provide a fresh, empty PageTable instance."""
    return PageTable()


@pytest.fixture
def sample_entries() -> List[PgTabEntry]:
    """Provide a list of sample PgTabEntry objects with known timestamps."""
    return [
        PgTabEntry(block_num=1, mem_slot_ix=1, acc_time=100),
        PgTabEntry(block_num=2, mem_slot_ix=2, acc_time=200),
        PgTabEntry(block_num=3, mem_slot_ix=3, acc_time=150),
        PgTabEntry(block_num=4, mem_slot_ix=4, acc_time=300),
    ]


def test_pgtabentry_comparison():
    """Test PgTabEntry comparison operations (note reversed for max-heap)."""
    entry1 = PgTabEntry(block_num=1, mem_slot_ix=1, acc_time=100)
    entry2 = PgTabEntry(block_num=2, mem_slot_ix=2, acc_time=200)

    # For max-heap, higher acc_time should be "less than" lower acc_time
    assert entry1 > entry2  # 100 makes entry1 "greater than" 200
    assert entry2 < entry1  # 200 makes entry2 "less than" 100

    # Test equality
    entry3 = PgTabEntry(block_num=3, mem_slot_ix=3, acc_time=100)
    assert entry1 >= entry3
    assert entry3 >= entry1


def test_push_heap(empty_page_table, sample_entries):
    """Test pushing entries maintains heap property."""
    pt = empty_page_table

    # Push all sample entries
    for entry in sample_entries:
        pt.do_push_heap(entry)
        assert pt.check_heap()  # Verify heap property after each push

    # Verify the size
    assert len(pt.pg_tab) == len(sample_entries)


def test_pop_heap(empty_page_table, sample_entries):
    """Test popping entries maintains heap order."""
    pt = empty_page_table

    # Push all entries first
    for entry in sample_entries:
        pt.do_push_heap(entry)

    # Pop entries and verify they come out in decreasing acc_time order
    times = []
    while pt.pg_tab:
        entry = pt.do_pop_heap()
        times.append(entry.acc_time)
        assert pt.check_heap()  # Verify heap property after each pop

    assert times == sorted(times, reverse=True)  # Should come out in decreasing order


def test_update_access_time(empty_page_table, sample_entries):
    """Test updating access times maintains heap property."""
    pt = empty_page_table

    # Add all entries
    for entry in sample_entries:
        pt.do_push_heap(entry)

    # Update access time of a middle entry
    middle_ix = len(pt.pg_tab) // 2
    pt.update_a_time(middle_ix)

    assert pt.check_heap()  # Verify heap property is maintained


def test_reset_access_time(empty_page_table, sample_entries):
    """Test resetting access times maintains heap property."""
    pt = empty_page_table

    # Add all entries
    for entry in sample_entries:
        pt.do_push_heap(entry)

    # Reset access time of a middle entry
    middle_ix = len(pt.pg_tab) // 2
    pt.reset_a_time(middle_ix)

    assert pt.check_heap()  # Verify heap property is maintained
    assert pt.pg_tab[middle_ix].acc_time == 0


def test_get_page_table_entry(empty_page_table, sample_entries):
    """Test retrieving page table entries."""
    pt = empty_page_table

    # Add all entries
    for entry in sample_entries:
        pt.do_push_heap(entry)

    # Verify we can retrieve each entry
    for i in range(len(pt.pg_tab)):
        entry = pt.get_pg_tab_entry(i)
        assert isinstance(entry, PgTabEntry)


def test_get_slot_from_memslot(empty_page_table, sample_entries):
    """Test finding page table slots from memory slots."""
    pt = empty_page_table

    # Add all entries
    for entry in sample_entries:
        pt.do_push_heap(entry)

    # Try to find each memory slot
    for entry in sample_entries:
        slot = pt.get_pg_tab_slot_frm_mem_slot(entry.mem_slot_ix)
        assert slot != -1
        assert pt.pg_tab[slot].mem_slot_ix == entry.mem_slot_ix


def test_is_leaf(empty_page_table, sample_entries):
    """Test leaf node identification."""
    pt = empty_page_table

    # Add all entries
    for entry in sample_entries:
        pt.do_push_heap(entry)

    # First half of nodes should not be leaves
    for i in range(len(pt.pg_tab) // 2):
        assert not pt.is_leaf(i)

    # Second half of nodes should be leaves
    for i in range(len(pt.pg_tab) // 2, len(pt.pg_tab)):
        assert pt.is_leaf(i)


def test_page_table_full_status(empty_page_table):
    """Test page table full status management."""
    pt = empty_page_table

    assert not pt.get_pg_tab_full()  # Should start not full

    pt.set_pg_tab_full()
    assert pt.get_pg_tab_full()


def test_heapify_from_unordered(empty_page_table):
    """Test heapifying an unordered page table."""
    pt = empty_page_table

    # Add entries in an order that violates heap property
    entries = [
        PgTabEntry(block_num=1, mem_slot_ix=1, acc_time=100),
        PgTabEntry(block_num=2, mem_slot_ix=2, acc_time=300),  # This should be at top
        PgTabEntry(block_num=3, mem_slot_ix=3, acc_time=200),
    ]

    pt.pg_tab = entries  # Directly set entries (normally would use push)
    assert not pt.check_heap()  # Should not be a valid heap

    pt.heapify()  # Fix the heap
    assert pt.check_heap()  # Should now be a valid heap


def test_empty_heap_operations(empty_page_table):
    """Test operations on an empty heap."""
    pt = empty_page_table

    assert pt.check_heap()  # Empty heap should be valid
    assert len(pt.pg_tab) == 0

    # Add and remove an entry
    entry = PgTabEntry(block_num=1, mem_slot_ix=1, acc_time=100)
    pt.do_push_heap(entry)
    assert len(pt.pg_tab) == 1

    popped = pt.do_pop_heap()
    assert len(pt.pg_tab) == 0
    assert popped.acc_time == entry.acc_time


def test_invalid_operations(empty_page_table):
    """Test handling of invalid operations."""
    pt = empty_page_table

    # Try to get slot for non-existent memory slot
    assert pt.get_pg_tab_slot_frm_mem_slot(999) == -1

    # Try to update access time for invalid index
    with pytest.raises(IndexError):
        pt.update_a_time(0)  # Should raise since page table is empty