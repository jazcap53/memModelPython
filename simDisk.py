import os
import struct
from typing import List, Optional
from ajTypes import u32Const, bNum_t, bNum_tConst, lNum_tConst, SENTINEL_INUM
from ajCrc import BoostCRC
from myMemory import Page
from status import Status
from journal import Journal
from inodeTable import InodeTable

class SimSector:
    def __init__(self):
        self.sect = bytearray(u32Const.BLOCK_BYTES.value)

class SimDisk:
    def __init__(self, p_stt: Status, dfn: str, jfn: str, ffn: str, nfn: str):
        self.dFileName = dfn
        self.jFileName = jfn
        self.fFileName = ffn
        self.nFileName = nfn
        self.p_stt = p_stt
        self.ds = None
        self.theDisk = [SimSector() for _ in range(bNum_tConst.NUM_DISK_BLOCKS.value)]
        self.errBlocks = []
        self.init()

    def __del__(self):
        if self.ds:
            self.ds.close()

    def init(self):
        self.p_stt.wrt("Initializing")

        success_disk = self.read_or_create(self.dFileName, u32Const.BLOCK_BYTES.value * bNum_tConst.NUM_DISK_BLOCKS.value, u32Const.BLOCK_BYTES.value, "disk")
        success_jrnl = self.read_or_create(self.jFileName, u32Const.BLOCK_BYTES.value * u32Const.PAGES_PER_JRNL.value, u32Const.BLOCK_BYTES.value, "jrnl")
        success_free = self.read_or_create(self.fFileName, u32Const.BLOCK_BYTES.value * u32Const.NUM_FREE_LIST_BLOCKS.value * 2 + 4, 0, "free")
        # success_node = self.read_or_create(self.nFileName, (u32Const.NUM_INODE_TBL_BLOCKS.value * u32Const.INODES_PER_BLOCK.value // 8) + (u32Const.BLOCK_BYTES.value * u32Const.NUM_INODE_TBL_BLOCKS.value), 0, "node")
        success_node = self.read_or_create(self.nFileName, (
                    u32Const.NUM_INODE_TBL_BLOCKS.value * lNum_tConst.INODES_PER_BLOCK.value // 8) + (
                                                       u32Const.BLOCK_BYTES.value * u32Const.NUM_INODE_TBL_BLOCKS.value),
                                           0, "node")

        if success_disk and success_jrnl and success_free and success_node:
            self.ds = open(self.dFileName, "r+b")
            if not self.ds:
                print(f"ERROR: Error opening file {self.dFileName} for update (r/w) in SimDisk::init()")
                exit(1)
        else:
            exit(1)

    def get_ds(self):
        return self.ds

    def get_d_file_name(self) -> str:
        return self.dFileName

    def do_create_block(self, s: bytearray, rWSz: bNum_t):
        self.create_block(s, rWSz)

    def read_or_create(self, fName: str, fSz: int, rWSz: bNum_t, fType: str) -> bool:
        if os.path.exists(fName):
            if os.path.getsize(fName) != fSz:
                print(f"ERROR: Bad file size for {fName} in SimDisk::readOrCreate()")
                return False
            else:
                return self.try_read(fName, fSz, rWSz, fType)
        else:
            return self.try_create(fName, rWSz, fType)

    def try_read(self, fName: str, fSz: int, rWSz: bNum_t, fType: str) -> bool:
        success = True
        with open(fName, "rb") as ifs:
            if fType == "disk":
                self.err_scan(ifs, rWSz)
                if ifs.tell() != fSz:
                    print(f"ERROR: Read error on disk file in SimDisk::tryRead()")
                    success = False
                if self.errBlocks:
                    self.process_errors()
        return success

    def try_create(self, fName: str, rWSz: bNum_t, fType: str) -> bool:
        success = True
        with open(fName, "wb") as ofs:
            if fType == "disk":
                self.create_d_file(ofs, rWSz)
            elif fType == "jrnl":
                self.create_j_file(ofs, rWSz)
            elif fType == "free":
                self.create_f_file(ofs)
            elif fType == "node":
                self.create_n_file(ofs)
        return success

    def create_d_file(self, ofs, rWSz: bNum_t):
        for s in self.theDisk:
            self.create_block(s.sect, rWSz)
            ofs.write(s.sect)

    @staticmethod
    def create_block(s: bytearray, rWSz: bNum_t):
        # Fill with a pattern (for testing purposes)
        for i in range(rWSz - u32Const.CRC_BYTES.value):
            s[i] = i % 256

        # Debug: Print block before CRC
        print(f"Block before CRC (last 8 bytes): {s[-8:].hex()}")

        crc = BoostCRC.get_code(s[:-u32Const.CRC_BYTES.value], rWSz - u32Const.CRC_BYTES.value)
        print(f"Calculated CRC: {crc:08x}")

        # Create a temporary bytearray for the last 4 bytes
        crc_target = bytearray(s[-u32Const.CRC_BYTES.value:])
        BoostCRC.wrt_bytes_little_e(crc, crc_target, u32Const.CRC_BYTES.value)

        # Copy the CRC bytes back to the original bytearray
        s[-u32Const.CRC_BYTES.value:] = crc_target

        # Debug: Dump block contents after CRC
        print(f"Block after CRC (last 8 bytes): {s[-8:].hex()}")

    @staticmethod
    def create_j_file(ofs, rWSz: bNum_t):
        uCArr = bytearray(u32Const.BLOCK_BYTES.value)
        for _ in range(u32Const.PAGES_PER_JRNL.value):
            ofs.write(uCArr)

    @staticmethod
    def create_f_file(ofs):
        bits_to_set = bNum_tConst.NUM_DISK_BLOCKS.value
        bytes_to_set = bits_to_set >> 3
        bFrm = bytearray(u32Const.BLOCK_BYTES.value * u32Const.NUM_FREE_LIST_BLOCKS.value)
        bTo = bytearray(u32Const.BLOCK_BYTES.value * u32Const.NUM_FREE_LIST_BLOCKS.value)

        arr_ix = 0
        while bytes_to_set > u32Const.BYTES_PER_PAGE.value:
            bFrm[arr_ix * u32Const.BYTES_PER_PAGE.value:(arr_ix + 1) * u32Const.BYTES_PER_PAGE.value] = b'\xff' * u32Const.BYTES_PER_PAGE.value
            arr_ix += 1
            bytes_to_set -= u32Const.BYTES_PER_PAGE.value

        bFrm[arr_ix * u32Const.BYTES_PER_PAGE.value:arr_ix * u32Const.BYTES_PER_PAGE.value + bytes_to_set] = b'\xff' * bytes_to_set

        init_posn = 0
        ofs.write(bFrm)
        ofs.write(bTo)
        ofs.write(struct.pack('<I', init_posn))

    # @staticmethod
    # def create_n_file(ofs):
    #     avl_arr_sz_bytes = u32Const.NUM_INODE_TBL_BLOCKS.value * u32Const.INODES_PER_BLOCK.value // 8
    #     avl_arr = bytearray(b'\xff' * avl_arr_sz_bytes)
    #     ofs.write(avl_arr)
    #
    #     b_nums_filler = SENTINEL_INUM.to_bytes(4, 'little') * u32Const.CT_INODE_BNUMS.value
    #     indirect_filler = SENTINEL_INUM.to_bytes(4, 'little') * u32Const.CT_INODE_INDIRECTS.value
    #     lkd_filler = SENTINEL_INUM.to_bytes(4, 'little')
    #     cr_time_filler = (0).to_bytes(8, 'little')
    #
    #     for i in range(u32Const.NUM_INODE_TBL_BLOCKS.value):
    #         for j in range(u32Const.INODES_PER_BLOCK.value):
    #             ix = i * u32Const.INODES_PER_BLOCK.value + j
    #             ofs.write(b_nums_filler)
    #             ofs.write(lkd_filler)
    #             ofs.write(cr_time_filler)
    #             ofs.write(indirect_filler)
    #             ofs.write(ix.to_bytes(4, 'little'))

    @staticmethod
    def create_n_file(ofs):
        avl_arr_sz_bytes = u32Const.NUM_INODE_TBL_BLOCKS.value * lNum_tConst.INODES_PER_BLOCK.value // 8
        avl_arr = bytearray(b'\xff' * avl_arr_sz_bytes)
        ofs.write(avl_arr)

        b_nums_filler = SENTINEL_INUM.to_bytes(4, 'little') * u32Const.CT_INODE_BNUMS.value
        indirect_filler = SENTINEL_INUM.to_bytes(4, 'little') * u32Const.CT_INODE_INDIRECTS.value
        lkd_filler = SENTINEL_INUM.to_bytes(4, 'little')
        cr_time_filler = (0).to_bytes(8, 'little')

        for i in range(u32Const.NUM_INODE_TBL_BLOCKS.value):
            for j in range(lNum_tConst.INODES_PER_BLOCK.value):
                ix = i * lNum_tConst.INODES_PER_BLOCK.value + j
                ofs.write(b_nums_filler)
                ofs.write(lkd_filler)
                ofs.write(cr_time_filler)
                ofs.write(indirect_filler)
                ofs.write(ix.to_bytes(4, 'little'))

    def err_scan(self, ifs, rWSz: bNum_t):
        for i, s in enumerate(self.theDisk):
            s.sect = ifs.read(rWSz)
            code = BoostCRC.get_code(s.sect, rWSz)
            if code:
                self.errBlocks.append(i)

    def process_errors(self):
        for bN in self.errBlocks:
            print(f"WARNING: Found data error in block {bN} on startup.")
        print()

@staticmethod
def cleanup_output_files(disk_file, jrnl_file, free_file, node_file):
    for file in [disk_file, jrnl_file, free_file, node_file]:
        if os.path.exists(file):
            os.remove(file)
    print("Output files cleaned up.")

if __name__ == "__main__":
    # Basic test setup
    class MockStatus:
        def wrt(self, msg):
            print(f"Status: {msg}")

    status = MockStatus()

    # Create files in the project root
    disk_file = "sim_disk.bin"
    jrnl_file = "sim_jrnl.bin"
    free_file = "sim_free.bin"
    node_file = "sim_node.bin"

    # Create SimDisk instance
    sim_disk = SimDisk(status, disk_file, jrnl_file, free_file, node_file)

    # Test basic functionality
    print(f"Disk file name: {sim_disk.get_d_file_name()}")

    # Test create_block
    test_block = bytearray(u32Const.BLOCK_BYTES.value)
    sim_disk.do_create_block(test_block, u32Const.BLOCK_BYTES.value)
    print(f"Created block CRC: {test_block[-4:]}")

    # Cleanup function (commented out by default)
    # SimDisk.cleanup_output_files(disk_file, jrnl_file, free_file, node_file)

    print("SimDisk tests completed.")