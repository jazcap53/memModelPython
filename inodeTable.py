import struct
from typing import List
from ajTypes import u32Const, lNum_tConst, bNum_t, inNum_t, SENTINEL_INUM, SENTINEL_BNUM
from ajUtils import get_cur_time, Tabber
from fileShifter import FileShifter
from arrBit import ArrBit


class Inode:
    def __init__(self):
        self.b_nums = [SENTINEL_BNUM] * u32Const.CT_INODE_BNUMS.value
        self.lkd = SENTINEL_INUM
        self.cr_time = 0
        self.indirect = [SENTINEL_BNUM] * u32Const.CT_INODE_INDIRECTS.value
        self.i_num = SENTINEL_INUM


class InodeTable:
    def __init__(self, nfn: str):
        self.file_name = nfn
        self.shifter = FileShifter()
        self.tabs = Tabber()
        self.avail = ArrBit(u32Const.NUM_INODE_TBL_BLOCKS.value, lNum_tConst.INODES_PER_BLOCK.value)
        self.avail.set()  # Initialize with all bits set (all inodes available)
        self.tbl = [[Inode() for _ in range(lNum_tConst.INODES_PER_BLOCK.value)]
                    for _ in range(u32Const.NUM_INODE_TBL_BLOCKS.value)]
        self.modified = False
        self.load_tbl()  # Load saved state if it exists

    def ref_tbl_node(self, i_num: inNum_t) -> Inode:
        blk_num = i_num // lNum_tConst.INODES_PER_BLOCK.value
        blk_ix = i_num % lNum_tConst.INODES_PER_BLOCK.value
        return self.tbl[blk_num][blk_ix]

    def assign_in_n(self) -> inNum_t:
        max_inum = u32Const.NUM_INODE_TBL_BLOCKS.value * lNum_tConst.INODES_PER_BLOCK.value
        for ix in range(max_inum):
            if self.avail.test(ix):
                self.avail.reset(ix)
                node = self.ref_tbl_node(ix)
                node.cr_time = get_cur_time(True)
                self.modified = True
                return ix
        print("No available inodes found.")
        return SENTINEL_INUM

    def release_in_n(self, i_num: inNum_t) -> None:
        if i_num == SENTINEL_INUM:
            return
        node = self.ref_tbl_node(i_num)
        node.b_nums = [SENTINEL_BNUM] * u32Const.CT_INODE_BNUMS.value
        node.cr_time = 0
        node.indirect = [SENTINEL_BNUM] * u32Const.CT_INODE_INDIRECTS.value
        self.avail.set(i_num)
        self.modified = True

    def node_in_use(self, i_num: inNum_t) -> bool:
        if i_num == SENTINEL_INUM:
            return False
        return not self.avail.test(i_num)

    def node_locked(self, i_num: inNum_t) -> bool:
        assert i_num != SENTINEL_INUM
        return self.ref_tbl_node(i_num).lkd != SENTINEL_INUM

    def assign_blk_n(self, i_num: inNum_t, blk: bNum_t) -> bool:
        assert i_num != SENTINEL_INUM
        assert not self.avail.test(i_num)
        node = self.ref_tbl_node(i_num)
        for i, item in enumerate(node.b_nums):
            if item == SENTINEL_BNUM:
                node.b_nums[i] = blk
                self.modified = True
                return True
        return False

    def release_blk_n(self, i_num: inNum_t, tgt: bNum_t) -> bool:
        if i_num != SENTINEL_INUM:
            node = self.ref_tbl_node(i_num)
            for i, item in enumerate(node.b_nums):
                if item == tgt:
                    node.b_nums[i] = SENTINEL_BNUM
                    print(f"{self.tabs(2, True)}Releasing block number {tgt} from inode {i_num}")
                    self.modified = True
                    return True
        return False

    def release_all_blk_n(self, i_num: inNum_t) -> None:
        if i_num != SENTINEL_INUM:
            node = self.ref_tbl_node(i_num)
            for i, item in enumerate(node.b_nums):
                if item != SENTINEL_BNUM:
                    self.release_blk_n(i_num, item)

    def list_all_blk_n(self, i_num: inNum_t) -> List[bNum_t]:
        if i_num == SENTINEL_INUM:
            print("InodeTable::list_all_blk_n() called with SENTINEL_INUM")
            return []
        return [item for item in self.ref_tbl_node(i_num).b_nums if item != SENTINEL_BNUM]

    def load_tbl(self) -> None:
        try:
            with open(self.file_name, 'rb') as f:
                # Read and set availability bitmap
                avail_bytes = f.read(u32Const.NUM_INODE_TBL_BLOCKS.value *
                                     lNum_tConst.INODES_PER_BLOCK.value // 8)
                self.avail = ArrBit.from_bytes(avail_bytes,
                                               u32Const.NUM_INODE_TBL_BLOCKS.value,
                                               lNum_tConst.INODES_PER_BLOCK.value)

                # Read inode table entries
                for i in range(u32Const.NUM_INODE_TBL_BLOCKS.value):
                    for j in range(lNum_tConst.INODES_PER_BLOCK.value):
                        node = Inode()
                        # Read direct block numbers
                        node.b_nums = list(
                            struct.unpack(f'<{u32Const.CT_INODE_BNUMS.value}I',
                                          f.read(4 * u32Const.CT_INODE_BNUMS.value)))
                        # Read lock status
                        node.lkd, = struct.unpack('<I', f.read(4))
                        # Read creation time
                        node.cr_time, = struct.unpack('<Q', f.read(8))
                        # Read indirect block numbers
                        node.indirect = list(struct.unpack(f'<{u32Const.CT_INODE_INDIRECTS.value}I',
                                                           f.read(4 * u32Const.CT_INODE_INDIRECTS.value)))
                        # Read inode number
                        node.i_num, = struct.unpack('<I', f.read(4))
                        self.tbl[i][j] = node

        except FileNotFoundError:
            print(f"Inode table file not found. Initializing with all inodes available.")
            # avail is already set to all 1s from __init__

    def store_tbl(self) -> None:
        def do_store_tbl(f):
            f.write(self.avail.to_bytes())
            for block in self.tbl:
                for node in block:
                    f.write(struct.pack(f'<{u32Const.CT_INODE_BNUMS.value}I', *node.b_nums))
                    f.write(struct.pack('<I', node.lkd))
                    f.write(struct.pack('<Q', node.cr_time))
                    f.write(struct.pack(f'<{u32Const.CT_INODE_INDIRECTS.value}I', *node.indirect))
                    f.write(struct.pack('<I', node.i_num))
            print(f"\n{self.tabs(1)}Inode table stored.")

        self.shifter.shift_files(self.file_name, do_store_tbl, binary_mode=True)

    def ensure_stored(self) -> None:
        if self.modified:
            self.store_tbl()
            self.modified = False


if __name__ == '__main__':
    # Test the InodeTable class
    import tempfile

    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_filename = temp_file.name

    # Initialize the inode table file with some dummy data
    with open(temp_filename, 'wb') as f:
        f.write(b'\x00' * (u32Const.NUM_INODE_TBL_BLOCKS.value * lNum_tConst.INODES_PER_BLOCK.value // 8))
        for _ in range(u32Const.NUM_INODE_TBL_BLOCKS.value * lNum_tConst.INODES_PER_BLOCK.value):
            f.write(
                struct.pack(f'<{u32Const.CT_INODE_BNUMS.value}I', *([SENTINEL_BNUM] * u32Const.CT_INODE_BNUMS.value)))
            f.write(struct.pack('<I', SENTINEL_INUM))
            f.write(struct.pack('<Q', 0))
            f.write(struct.pack(f'<{u32Const.CT_INODE_INDIRECTS.value}I',
                                *([SENTINEL_BNUM] * u32Const.CT_INODE_INDIRECTS.value)))
            f.write(struct.pack('<I', SENTINEL_INUM))

    inode_table = InodeTable(temp_filename)

    # Test assign_in_n and node_in_use
    inode_num = inode_table.assign_in_n()
    print(f"Assigned inode number: {inode_num}")
    if inode_num != SENTINEL_INUM:
        print(f"Is node {inode_num} in use? {inode_table.node_in_use(inode_num)}")

        # Test assign_blk_n and list_all_blk_n
        inode_table.assign_blk_n(inode_num, 42)
        print(f"Blocks assigned to inode {inode_num}: {inode_table.list_all_blk_n(inode_num)}")

        # Test release_blk_n
        inode_table.release_blk_n(inode_num, 42)
        print(f"Blocks after release: {inode_table.list_all_blk_n(inode_num)}")

        # Test release_in_n
        inode_table.release_in_n(inode_num)
        print(f"Is node {inode_num} in use after release? {inode_table.node_in_use(inode_num)}")
    else:
        print("No available inodes to test with.")

    # Explicitly store the table before exiting
    inode_table.store_tbl()

    # Clean up
    import os

    os.unlink(temp_filename)