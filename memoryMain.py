"""
memoryMain.py

This is the main entry point for the memory simulation program. It coordinates
the interaction between various components such as the crash checker, driver,
main memory, change log, status tracker, simulated disk, journal, memory manager,
file manager, and client.

The program simulates a memory management system with journaling capabilities,
allowing for crash recovery and persistent storage of data. It orchestrates
the flow of data between memory and disk, manages file operations, and ensures
data consistency through journaling.

This implementation corresponds to the original C++ memoryMain.cpp, adapted
to Python while maintaining the core functionality and component interactions.
"""

import sys
from crashChk import CrashChk
from driver import Driver
from myMemory import Memory
from change import ChangeLog
from status import Status
from simDisk import SimDisk
from journal import Journal
from memMan import MemMan
from fileMan import FileMan
from client import Client

# TODO: to implement centralized logging, comment in these lines and add the lines
# TODO:     below # ============== to each module, replacing its current scheme
#
# from logging_config import setup_logging, get_logger
#
# # Set up logging at the start of your program
# setup_logging()
#
# # Get a logger for this module
# logger = get_logger(__name__)
# ========================
# from logging_config import get_logger
#
# logger = get_logger(__name__)




def main(argv):
    # Initialize crash checker to help system recover gracefully from a crash
    crash_checker = CrashChk()

    # Set up the program with command line arguments
    driver = Driver(argv)

    # Set up data structures for pages and main memory
    main_memory = Memory()

    # Track changes for write to journal
    change_log = ChangeLog(driver.get_test())

    # Maintain a status file for program
    my_status = Status(driver.get_s_file_name())

    # Set up files for disk, journal, free list, and inode table
    sim_disk = SimDisk(my_status,
                       driver.get_d_file_name(),
                       driver.get_j_file_name(),
                       driver.get_f_file_name(),
                       driver.get_n_file_name())

    # Write changes from change log to journal, and from journal to disk
    journal = Journal(driver.get_j_file_name(),
                     sim_disk,
                     change_log,
                     my_status,
                     crash_checker)

    # Coordinate memory management
    mem_manager = MemMan(main_memory,
                         sim_disk,
                         journal,
                         change_log,
                         my_status,
                         driver.get_verbose())

    # Set up file system management
    file_manager = FileMan(driver.get_n_file_name(),
                          driver.get_f_file_name(),
                          mem_manager)

    # Set up client to make requests
    client = Client(0, file_manager, driver)

    # Start processing client requests
    client.make_requests()


if __name__ == "__main__":
    main(sys.argv)