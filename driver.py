"""
driver.py: Setup and Configuration Module

This module is responsible for setting up and configuring the memory model simulation.
It handles command-line arguments, initializes the system components, and manages the
overall program setup. The Driver class, along with the main function, prepares the
program for operation by:

1. Processing command-line arguments
2. Setting up logging and output files
3. Initializing system parameters
4. Providing access to configuration settings for other modules

This module serves as the entry point for system configuration and initialization.
"""

import sys
import time
from datetime import datetime
import random
from typing import List, Optional

from ajTypes import bNum_t, u32Const
from memMan import MemMan
from change import Change
from status import Status
from ajUtils import Tabber, get_cur_time


class Driver:
    # Constants
    SHORT_RUN = 256  # convenient value chosen by trial-and-error
    RUN_FACTOR = 112

    def __init__(self, args: List[str]):
        # Command line switches
        self.verbose = False
        self.test = False
        self.long_run = False
        self.help = False

        # Default seed for the Random Engine
        self.the_seed = 7900

        self.tabs = Tabber()
        self.log_file = None
        self.backup_buf = None

        self.init(args)

    def init(self, args: List[str]):
        self.rd_cl_args(args)
        from ajUtils import set_test_mode  # Import here to avoid circular imports
        set_test_mode(self.test)  # Set test mode based on -t switch

        if self.help:
            self.display_help()
            sys.exit(0)

        self.log_file = open("output.txt", "w")
        self.wrt_header(args, "OUTPUT", self.log_file)
        self.wrt_header(args, "ERROR OUTPUT", sys.stderr)

    def rd_cl_args(self, args: List[str]):
        i = 1  # Skip program name
        while i < len(args):
            if args[i].startswith('-'):
                flag = args[i][1]
                if flag == 'v':
                    self.verbose = True
                elif flag == 't':
                    self.test = True
                elif flag == 's':
                    if i + 1 < len(args) and args[i + 1].isdigit():
                        self.the_seed = int(args[i + 1])
                        i += 1
                    else:
                        self.help = True
                elif flag in ['L', 'l']:
                    self.long_run = True
                elif flag == 'h':
                    self.help = True
                else:
                    print(f"ERROR: Bad command line argument: {args[i]}", file=sys.stderr)
            i += 1

    def wrt_header(self, args: List[str], tag: str, output_file):
        output_file.write(f"{self.tabs(0)}{tag}: {' '.join(args)}: {datetime.now()}\n")

    def display_help(self):
        help_text = """
        memModel options:

        -h   Help       Print this help and exit.

        -l
        -L   Long run   Run the program long enough to make the
                       journal file wrap around.

        -s   Seed       Seed the random number generator with the
                       non-negative integer that appears as the next
                       command line argument.

        -t   Test       Use the default seed (or the argument to -s,
                       if given) for the random number generator.
                       Make the first line of each output change hold
                       its block number.

        -v   Verbose    Send some extra debugging information to stdout.
        """
        print(help_text)

    def get_d_file_name(self) -> str:
        return "disk_file.bin"

    def get_j_file_name(self) -> str:
        return "jrnl_file.bin"

    def get_f_file_name(self) -> str:
        return "free_file.bin"

    def get_n_file_name(self) -> str:
        return "node_file.bin"

    def get_s_file_name(self) -> str:
        return "status.txt"

    def get_verbose(self) -> bool:
        return self.verbose

    def get_test(self) -> bool:
        return self.test

    def get_long_run(self) -> bool:
        return self.long_run

    def get_the_seed(self) -> int:
        return self.the_seed

    def __del__(self):
        if self.log_file:
            self.log_file.close()


if __name__ == "__main__":
    # Test the Driver class
    test_args = ["memModel", "-v", "-t", "-s", "12345", "-l"]

    driver = Driver(test_args)

    print(f"Verbose mode: {driver.get_verbose()}")
    print(f"Test mode: {driver.get_test()}")
    print(f"Long run: {driver.get_long_run()}")
    print(f"Seed: {driver.get_the_seed()}")

    print(f"Disk file name: {driver.get_d_file_name()}")
    print(f"Journal file name: {driver.get_j_file_name()}")
    print(f"Free list file name: {driver.get_f_file_name()}")
    print(f"Inode table file name: {driver.get_n_file_name()}")
    print(f"Status file name: {driver.get_s_file_name()}")

    # Test with help flag
    help_args = ["memModel", "-h"]
    help_driver = Driver(help_args)