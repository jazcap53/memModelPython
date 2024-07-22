from typing import List
from dataclasses import dataclass
from ajTypes import u32Const, bNum_t
from ajUtils import get_cur_time, Tabber

@dataclass
class PgTabEntry:
    block_num: bNum_t = 0
    mem_slot_ix: int = 0
    acc_time: int = 0

def pte_comp(l: PgTabEntry, r: PgTabEntry) -> bool:
    return r.acc_time < l.acc_time

class PageTable:
    def __init__(self):
        self.heap_size = 0
        self.pg_tab_full = False
        self.pg_tab: List[PgTabEntry] = [PgTabEntry() for _ in range(u32Const.NUM_MEM_SLOTS.value)]
        self.tabs = Tabber()

    def update_a_time(self, loc: int) -> None:
        self.pg_tab[loc].acc_time = get_cur_time()

    def reset_a_time(self, loc: int) -> None:
        self.pg_tab[loc].acc_time = 0

    def get_pg_tab_entry(self, loc: int) -> PgTabEntry:
        return self.pg_tab[loc]

    def get_pg_tab_slot_frm_mem_slot(self, mem_slot: int) -> int:
        for i, entry in enumerate(self.pg_tab[:self.heap_size]):
            if entry.mem_slot_ix == mem_slot:
                return i
        return -1

    def is_leaf(self, mem_slot: int) -> bool:
        return mem_slot >= self.heap_size // 2

    def heapify(self) -> None:
        self._heapify(0)

    def _heapify(self, i: int) -> None:
        largest = i
        left = 2 * i + 1
        right = 2 * i + 2

        if left < self.heap_size and pte_comp(self.pg_tab[largest], self.pg_tab[left]):
            largest = left

        if right < self.heap_size and pte_comp(self.pg_tab[largest], self.pg_tab[right]):
            largest = right

        if largest != i:
            self.pg_tab[i], self.pg_tab[largest] = self.pg_tab[largest], self.pg_tab[i]
            self._heapify(largest)

    def do_pop_heap(self) -> PgTabEntry:
        assert self.heap_size > 0
        popped = self.pg_tab[0]
        self.pg_tab[0] = self.pg_tab[self.heap_size - 1]
        self.heap_size -= 1
        self._heapify(0)
        return popped

    def do_push_heap(self, new_entry: PgTabEntry) -> None:
        assert self.heap_size < u32Const.NUM_MEM_SLOTS.value
        self.heap_size += 1
        i = self.heap_size - 1
        self.pg_tab[i] = new_entry

        while i > 0 and pte_comp(self.pg_tab[(i - 1) // 2], self.pg_tab[i]):
            self.pg_tab[i], self.pg_tab[(i - 1) // 2] = self.pg_tab[(i - 1) // 2], self.pg_tab[i]
            i = (i - 1) // 2

    def check_heap(self) -> bool:
        return all(not pte_comp(self.pg_tab[(i - 1) // 2], self.pg_tab[i])
                   for i in range(1, self.heap_size))

    def set_pg_tab_full(self) -> None:
        self.pg_tab_full = True

    def get_pg_tab_full(self) -> bool:
        return self.pg_tab_full

    def print(self) -> None:
        print(f"\n{self.tabs(1, True)}Contents of page table:")
        for i, p in enumerate(self.pg_tab):
            print(f"{self.tabs(2, True)}pgTblIx: {i:2}   block: {p.block_num}"
                  f"   memSlotIx: {p.mem_slot_ix}   accTime: {p.acc_time:6}")

if __name__ == '__main__':
    # Basic tests
    pt = PageTable()

    # Test push and pop
    pt.do_push_heap(PgTabEntry(1, 1, 100))
    pt.do_push_heap(PgTabEntry(2, 2, 200))
    pt.do_push_heap(PgTabEntry(3, 3, 50))

    print("Heap after pushes:")
    pt.print()

    print("\nPopped entry:", pt.do_pop_heap())
    print("\nHeap after pop:")
    pt.print()

    # Test update and reset access time
    pt.update_a_time(0)
    print("\nAfter updating access time of entry 0:")
    pt.print()

    pt.reset_a_time(0)
    print("\nAfter resetting access time of entry 0:")
    pt.print()

    # Test get_pg_tab_slot_frm_mem_slot
    print("\nPgTabSlot for MemSlot 2:", pt.get_pg_tab_slot_frm_mem_slot(2))

    # Test heap property
    print("\nIs heap valid?", pt.check_heap())