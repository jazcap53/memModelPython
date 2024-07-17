from ajTypes import bNum_t, u32Const
from arrBit import ArrBit

class WipeList:
    DIRTY_BEFORE_WIPE = 16

    def __init__(self):
        self.dirty = ArrBit(u32Const.NUM_WIPE_PAGES.value, u32Const.BITS_PER_PAGE.value)
        self.clear_array()

    def set_dirty(self, b_num: bNum_t) -> None:
        self.dirty.set(b_num)

    def is_dirty(self, b_num: bNum_t) -> bool:
        return self.dirty.test(b_num)

    def clear_array(self) -> None:
        self.dirty.reset()

    def is_ripe(self) -> bool:
        num_to_wipe = self.dirty.count()
        return num_to_wipe >= self.DIRTY_BEFORE_WIPE


if __name__ == '__main__':
    # Test the WipeList functionality
    wl = WipeList()
    wl.set_dirty(5)
    wl.set_dirty(10)
    print(f"Is block 5 dirty? {wl.is_dirty(5)}")
    print(f"Is block 7 dirty? {wl.is_dirty(7)}")
    print(f"Is list ripe for cleaning? {wl.is_ripe()}")
    wl.clear_array()
    print(f"After clearing, is block 5 still dirty? {wl.is_dirty(5)}")