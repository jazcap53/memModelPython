from typing import List, Optional
from inodeTable import InodeTable, Inode
from freeList import FreeList
from ajTypes import inNum_t, bNum_t, SENTINEL_INUM, SENTINEL_BNUM, u32Const, lNum_tConst
from ajUtils import get_cur_time, Tabber
from change import Change


class FileMan:
    def __init__(self, nfn: str, ffn: str, pmm):
        self.iTbl = InodeTable(nfn)  # this line is correct
        try:
            self.frLst = FreeList(ffn)
        except Exception as e:
            print(f"Error initializing FreeList: {e}")
            raise
        self.p_mM = pmm
        self.any_dirty = False
        self.tabs = Tabber()

    def create_file(self) -> inNum_t:
        ret = self.iTbl.create_inode()  # was assign_in_n()
        if ret == SENTINEL_INUM:
            print("Unable to create file in FileMan::create_file(); file number limit reached.")
        else:
            print(f"{self.tabs(2, True)}File created with inode #{ret}")
        return ret

    def delete_file(self, cli_id: int, i_num: inNum_t) -> bool:
        if self.iTbl.is_locked(i_num):  # was node_locked()
            print(f"Unable to delete file {i_num} for client {cli_id} at time {get_cur_time()}: file locked.")
            return False
        if not self.file_exists(i_num):
            print(f"Unable to delete file {i_num} for client {cli_id} at time {get_cur_time()}: no such file.")
            return False

        # Get list of blocks before deleting the inode
        inode = self.iTbl.storage.get_inode(i_num)
        b_n_list = self.iTbl.block_manager.list_blocks(inode)  # was list_all_blk_n()

        for item in b_n_list:
            self.frLst.put_blk(item)
            self.p_mM.evict_this_page(item)

        for block in b_n_list:
            self.iTbl.release_block(i_num, block)  # was release_blk_n()
        self.iTbl.delete_inode(i_num)  # was release_in_n()

        print(f"{self.tabs(2, True)}File deleted with inode #{i_num} for client {cli_id} at time {get_cur_time()}")
        return True

    def count_files(self) -> inNum_t:
        files = 0
        total_inodes = u32Const.NUM_INODE_TBL_BLOCKS.value * lNum_tConst.INODES_PER_BLOCK.value
        for i in range(total_inodes):
            if self.iTbl.is_in_use(i):  # was avail.test()
                files += 1
        return files

    def count_blocks(self, i_num: inNum_t) -> bNum_t:
        inode = self.iTbl.storage.get_inode(i_num)  # was ref_tbl_node()
        return len(self.iTbl.block_manager.list_blocks(inode))  # was counting b_nums directly

    def file_exists(self, i_num: inNum_t) -> bool:
        if i_num == SENTINEL_INUM:
            return False
        return self.iTbl.is_in_use(i_num)  # was node_in_use()

    def block_exists(self, i_num: inNum_t, b_num: bNum_t) -> bool:
        if i_num == SENTINEL_INUM or b_num == SENTINEL_BNUM:
            return False
        if not self.file_exists(i_num):
            return False
        inode = self.iTbl.storage.get_inode(i_num)  # was ref_tbl_node()
        return b_num in inode.b_nums

    def add_block(self, cli_id: int, i_num: inNum_t) -> bNum_t:
        if self.iTbl.is_locked(i_num):  # was node_locked()
            print(f"Unable to add block to file {i_num} for client {cli_id} at time {get_cur_time()}: file locked.")
            return SENTINEL_BNUM

        b_num = self.frLst.get_blk()
        if b_num == SENTINEL_BNUM:
            print(f"Unable to add block to inode {i_num} for client {cli_id} at time {get_cur_time()}: no free blocks")
            return SENTINEL_BNUM

        if self.iTbl.assign_block(i_num, b_num):  # was assign_blk_n()
            self.p_mM.p_j.do_wipe_routine(b_num, self)
            print(
                f"{self.tabs(2, True)}Block {b_num} added to inode {i_num} for client {cli_id} at time {get_cur_time()}")
            return b_num
        else:
            print(
                f"Unable to add block to inode {i_num} for client {cli_id} at time {get_cur_time()}: no space in inode")
            return SENTINEL_BNUM

    def remv_block(self, cli_id: int, i_num: inNum_t, tgt: bNum_t) -> bool:
        if self.iTbl.is_locked(i_num):  # was node_locked()
            print(
                f"Unable to remove block {tgt} from inode {i_num} for client {cli_id} at time {get_cur_time()}: File locked.")
            return False

        if not self.iTbl.is_in_use(i_num):  # was node_in_use()
            print(
                f"Unable to remove block {tgt} from inode {i_num} for client {cli_id} at time {get_cur_time()}: Inode not in use.")
            return False

        if self.iTbl.release_block(i_num, tgt):  # was release_blk_n()
            self.remv_block_clean(tgt)
            print(f"{self.tabs(2)}Block {tgt} removed from inode {i_num} for client {cli_id} at time {get_cur_time()}")
            return True
        else:
            print(
                f"Unable to remove block {tgt} from inode {i_num} for client {cli_id} at time {get_cur_time()}: Block not found in inode")
            return False

    def remv_block_clean(self, tgt: bNum_t):
        self.frLst.put_blk(tgt)
        self.p_mM.evict_this_page(tgt)
        if self.p_mM.p_cL.is_in_log(tgt) or self.p_mM.p_j.is_in_jrnl(tgt):
            self.p_mM.p_j.set_wiper_dirty(tgt)

    def get_inode(self, i_num: inNum_t) -> Inode:
        assert i_num != SENTINEL_INUM
        return self.iTbl.storage.get_inode(i_num)  # was ref_tbl_node()

    def submit_request(self, do_wrt: bool, cli_id: int, i_num: inNum_t, cg: Change):
        locked = self.iTbl.is_locked(i_num)  # was node_locked()
        if not locked:
            self.p_mM.process_request(cg, self)
            self.any_dirty = True

    def do_store_inodes(self):
        self.iTbl.store()  # was store_tbl()

    def do_store_free_list(self):
        self.frLst.store_lst()


if __name__ == "__main__":
    # Mock MemMan class for testing
    class MockMemMan:
        def __init__(self):
            self.p_j = self.MockJournal()
            self.p_cL = self.MockChangeLog()

        def evict_this_page(self, page):
            print(f"Evicting page {page}")

        def process_request(self, cg, file_man):
            print(f"Processing request for block {cg.block_num}")

        class MockJournal:
            def do_wipe_routine(self, b_num, file_man):
                print(f"Wiping routine for block {b_num}")

            def is_in_jrnl(self, tgt):
                return False

            def set_wiper_dirty(self, tgt):
                print(f"Setting wiper dirty for block {tgt}")

        class MockChangeLog:
            def is_in_log(self, tgt):
                return False

    try:
        # Test the FileMan class
        mem_man = MockMemMan()
        file_man = FileMan("inode_table.bin", "free_list.bin", mem_man)

        # Test create_file
        inode_num = file_man.create_file()
        print(f"Created file with inode number: {inode_num}")

        # Test add_block
        block_num = file_man.add_block(1, inode_num)
        print(f"Added block number: {block_num}")

        # Test count_blocks
        block_count = file_man.count_blocks(inode_num)
        print(f"Block count for inode {inode_num}: {block_count}")

        # Test file_exists
        print(f"File exists: {file_man.file_exists(inode_num)}")

        # Test remv_block
        file_man.remv_block(1, inode_num, block_num)

        # Test delete_file
        file_man.delete_file(1, inode_num)

        # Test count_files
        file_count = file_man.count_files()
        print(f"Total file count: {file_count}")

        # Test submit_request
        cg = Change(block_num)
        file_man.submit_request(True, 1, inode_num, cg)

        # Test do_store_inodes and do_store_free_list
        file_man.do_store_inodes()
        file_man.do_store_free_list()

        print("FileMan tests completed successfully.")
    except Exception as e:
        print(f"An error occurred during testing: {e}")
    finally:
        print("FileMan tests completed.")