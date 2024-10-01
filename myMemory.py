from typing import List
from ajTypes import u32Const

class Page:
    def __init__(self):
        self.dat = bytearray(u32Const.BYTES_PER_PAGE.value)

class Memory:
    def __init__(self):
        self.num_mem_slots = u32Const.NUM_MEM_SLOTS.value
        self.first_avl_mem_slt = 0
        self.the_mem = [Page() for _ in range(self.num_mem_slots)]
        self.avl_mem_slts = [True] * self.num_mem_slots

    def get_first_avl_mem_slt(self) -> int:
        for i, is_available in enumerate(self.avl_mem_slts):
            if is_available:
                self.avl_mem_slts[i] = False
                return i
        return u32Const.NUM_MEM_SLOTS.value

    def make_avl_mem_slt(self, mem_slt: int) -> bool:
        if not self.avl_mem_slts[mem_slt]:
            self.avl_mem_slts[mem_slt] = True
            return True
        return False

    def take_avl_mem_slt(self, mem_slt: int) -> bool:
        if self.avl_mem_slts[mem_slt]:
            self.avl_mem_slts[mem_slt] = False
            return True
        return False

    def get_page(self, ix: int) -> Page:
        return self.the_mem[ix]


if __name__ == '__main__':
    # Test the Memory class
    memory = Memory()

    # Test get_first_avl_mem_slt
    print("Testing get_first_avl_mem_slt:")
    for i in range(5):
        slt = memory.get_first_avl_mem_slt()
        print(f"Got slot: {slt}")

    # Test make_avl_mem_slt
    print("\nTesting make_avl_mem_slt:")
    print(f"Make slot 2 available: {memory.make_avl_mem_slt(2)}")
    print(f"Make slot 2 available again: {memory.make_avl_mem_slt(2)}")

    # Test take_avl_mem_slt
    print("\nTesting take_avl_mem_slt:")
    print(f"Take slot 2: {memory.take_avl_mem_slt(2)}")
    print(f"Take slot 2 again: {memory.take_avl_mem_slt(2)}")

    # Test get_page
    print("\nTesting get_page:")
    page = memory.get_page(0)
    print(f"Got page, data length: {len(page.dat)}")
