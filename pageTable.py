import heapq
from typing import List
from dataclasses import dataclass
from ajTypes import u32Const, bNum_t
from ajUtils import get_cur_time, Tabber


@dataclass
class PgTabEntry:
    block_num: bNum_t = 0
    mem_slot_ix: int = 0
    acc_time: int = 0

    def __lt__(self, other):
        return self.acc_time > other.acc_time  # Note: reversed for max-heap behavior

    def __le__(self, other):
        return self.acc_time >= other.acc_time  # Note: reversed for max-heap behavior


class PageTable:
    def __init__(self):
        self.pg_tab: List[PgTabEntry] = []
        self.pg_tab_full = False
        self.tabs = Tabber()

    def update_a_time(self, loc: int) -> None:
        self.pg_tab[loc].acc_time = get_cur_time()
        heapq._siftdown(self.pg_tab, 0, loc)

    def reset_a_time(self, loc: int) -> None:
        self.pg_tab[loc].acc_time = 0
        heapq._siftup(self.pg_tab, loc)

    def get_pg_tab_entry(self, loc: int) -> PgTabEntry:
        return self.pg_tab[loc]

    def get_pg_tab_slot_frm_mem_slot(self, mem_slot: int) -> int:
        for i, entry in enumerate(self.pg_tab):
            if entry.mem_slot_ix == mem_slot:
                return i
        return -1

    def is_leaf(self, mem_slot: int) -> bool:
        return mem_slot >= len(self.pg_tab) // 2

    def heapify(self) -> None:
        heapq.heapify(self.pg_tab)

    def do_pop_heap(self) -> PgTabEntry:
        return heapq.heappop(self.pg_tab)

    def do_push_heap(self, new_entry: PgTabEntry) -> None:
        heapq.heappush(self.pg_tab, new_entry)

    def check_heap(self) -> bool:
        return all(self.pg_tab[(i - 1) // 2] <= self.pg_tab[i] for i in range(1, len(self.pg_tab)))

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
    pt = PageTable()

    # Test push (this should maintain heap property)
    pt.do_push_heap(PgTabEntry(1, 1, 200))
    pt.do_push_heap(PgTabEntry(2, 2, 100))
    pt.do_push_heap(PgTabEntry(3, 3, 150))

    print("Heap after pushes:")
    pt.print()
    print("\nIs heap valid?", pt.check_heap())  # Should print True

    # Manually break the heap property
    pt.pg_tab[0].acc_time = 50  # This violates the max-heap property

    print("\nHeap after manually breaking heap property:")
    pt.print()
    print("\nIs heap valid?", pt.check_heap())  # Should print False

    # Fix the heap
    pt.heapify()

    print("\nHeap after fixing:")
    pt.print()
    print("\nIs heap valid?", pt.check_heap())  # Should print True

    # Test pop
    popped = pt.do_pop_heap()
    print(f"\nPopped entry: {popped}")
    print("\nHeap after pop:")
    pt.print()
    print("\nIs heap valid?", pt.check_heap())  # Should print True
