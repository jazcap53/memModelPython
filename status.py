# status.py

"""
status.py

This module provides the Status class for reading and writing status messages to a file.
"""

import os
from fileShifter import FileShifter
import logging


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

        # Check if we have write permission before attempting to write
        if os.path.exists(self.file_name) and not os.access(self.file_name, os.W_OK):
            logging.error(f"No write permission for file {self.file_name}")
            return -1

        try:
            # Try to replace the content
            had_error = self.replace(msg)
        except (IOError, PermissionError):
            try:
                # If replace fails, try to create/write the file
                with open(self.file_name, 'w') as f:
                    f.write(f"{msg}\n")
            except (IOError, PermissionError):
                logging.error(f"Can't open file {self.file_name} for write of message {msg}")
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

        return self.shifter.shift_files(self.file_name, lambda f: do_replace(f))