"""
status.py

This module provides the Status class for reading and writing status messages to a file.
"""

import os
from fileShifter import FileShifter

class Status:
    """
    A class for reading and writing status messages to a file.

    This class uses the FileShifter class to perform atomic file updates,
    ensuring that the status file is always in a consistent state, even if
    the write operation is interrupted.
    """

    def __init__(self, sfn: str):
        """
        Initialize a Status instance.

        Args:
            sfn (str): The name of the status file.
        """
        self.file_name = sfn
        self.shifter = FileShifter()

    def rd(self) -> str:
        """
        Read the status message from the file.

        Returns:
            str: The status message, or an error message if the file cannot be read.
        """
        try:
            with open(self.file_name, 'r') as f:
                return f.readline().strip()
        except IOError:
            return "ERROR: Can not open status file for read in Status.rd()."

    def wrt(self, msg: str) -> int:
        """
        Write a status message to the file.

        Args:
            msg (str): The status message to write.

        Returns:
            int: 0 on success, or a non-zero error code on failure.
        """
        had_error = 0

        try:
            with open(self.file_name, 'r'):
                had_error = self.replace(msg)
        except IOError:
            try:
                with open(self.file_name, 'w') as f:
                    f.write(f"{msg}\n")
            except IOError:
                print(f"ERROR: Can't open file {self.file_name} for write of message {msg}")
                had_error = -1

        return had_error

    def replace(self, s: str) -> int:
        """
        Replace the contents of the status file with a new message.

        Args:
            s (str): The new status message.

        Returns:
            int: The return value of the FileShifter's shift_files method.
        """
        def do_replace(fs):
            fs.write(f"{s}\n")

        return self.shifter.shift_files(self.file_name, do_replace)


if __name__ == '__main__':
    # Basic test
    status = Status("test_status.txt")
    status.wrt("Initial status")
    print(f"Current status: {status.rd()}")
    status.wrt("Updated status")
    print(f"Updated status: {status.rd()}")
    os.remove("test_status.txt")