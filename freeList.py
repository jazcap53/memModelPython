import struct
from ajTypes import u32Const, bNum_t, SENTINEL_BNUM, bNum_tConst
from ajUtils import Tabber
from arrBit import ArrBit
import logging


logger = logging.getLogger(__name__)


class FreeList:
    def __init__(self, ffn: str, quiet: bool = False):
        self.quiet = quiet
        self.ffn = ffn  # Store the filename
        self.bitsFrm = ArrBit(u32Const.NUM_FREE_LIST_BLOCKS.value, u32Const.BITS_PER_PAGE.value)
        self.bitsTo = ArrBit(u32Const.NUM_FREE_LIST_BLOCKS.value, u32Const.BITS_PER_PAGE.value)
        self.fromPosn = 0
        self.tabs = Tabber()

        try:
            self.frs = open(ffn, "r+b")
            self.load_lst()
        except FileNotFoundError:
            print(f"Free list file not found. Initializing new file.")
            self.initialize_new_file()

    def initialize_new_file(self):
        self.frs = open(self.ffn, "w+b")
        self.bitsFrm.set()  # Set all blocks as free initially
        self.bitsTo.reset()  # Clear bitsTo
        self.fromPosn = 0
        self.store_lst()

    def __del__(self):
        if hasattr(self, 'frs'):
            self.store_lst()
            self.frs.close()

    def load_lst(self):
        self.frs.seek(0)
        bitsFrm_data = self.frs.read(self.bitsFrm.size() // 8)
        bitsTo_data = self.frs.read(self.bitsTo.size() // 8)
        fromPosn_data = self.frs.read(4)

        self.bitsFrm = ArrBit.from_bytes(bitsFrm_data, u32Const.NUM_FREE_LIST_BLOCKS.value,
                                         u32Const.BITS_PER_PAGE.value)
        self.bitsTo = ArrBit.from_bytes(bitsTo_data, u32Const.NUM_FREE_LIST_BLOCKS.value, u32Const.BITS_PER_PAGE.value)
        self.fromPosn = struct.unpack('<I', fromPosn_data)[0]

    def store_lst(self):
        self.frs.seek(0)
        self.frs.write(self.bitsFrm.to_bytes())
        self.frs.write(self.bitsTo.to_bytes())
        self.frs.write(struct.pack('<I', self.fromPosn))
        if not self.quiet:
            logger.info("Free list stored.")

    def get_blk(self) -> bNum_t:
        if self.fromPosn == bNum_tConst.NUM_DISK_BLOCKS.value:
            if self.bitsTo.any():
                self.refresh()

        found = False
        if self.fromPosn < bNum_tConst.NUM_DISK_BLOCKS.value:
            self.bitsFrm.reset(self.fromPosn)
            found = True
            self.fromPosn +=  1  # NEW

        result = self.fromPosn - 1 if found else SENTINEL_BNUM
        logger.debug(f"Got block: {result}")
        logger.debug(f"fromPosn after get: {self.fromPosn}")
        return result

    def refresh(self):
        self.bitsFrm |= self.bitsTo
        self.bitsTo.reset()

        self.fromPosn = 0
        for i in range(bNum_tConst.NUM_DISK_BLOCKS.value):
            if not self.bitsFrm.test(i):
                self.fromPosn += 1
            else:
                break

    def put_blk(self, bN: bNum_t):
        assert bN < bNum_tConst.NUM_DISK_BLOCKS.value
        self.bitsTo.set(bN)


if __name__ == "__main__":
    import tempfile
    import os

    # Create a temporary file for testing
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_filename = temp_file.name

    # Initialize the file with some dummy data
    with open(temp_filename, 'wb') as f:
        dummy_data = b'\x00' * (u32Const.NUM_FREE_LIST_BLOCKS.value * u32Const.BITS_PER_PAGE.value // 8 * 2 + 4)
        f.write(dummy_data)

    # Test FreeList
    free_list = FreeList(temp_filename)

    print("Testing FreeList:")

    # Test get_blk
    print("\nTesting get_blk:")
    for _ in range(5):
        block = free_list.get_blk()
        print(f"Got block: {block}")

    # Test put_blk
    print("\nTesting put_blk:")
    free_list.put_blk(2)
    free_list.put_blk(4)
    print("Put blocks 2 and 4 back into the free list")

    # Test refresh
    print("\nTesting refresh:")
    free_list.refresh()
    print("Refreshed free list")

    # Get blocks again to see if the returned blocks are reused
    print("\nGetting blocks after refresh:")
    for _ in range(5):
        block = free_list.get_blk()
        print(f"Got block: {block}")

    # Clean up
    del free_list
    os.unlink(temp_filename)