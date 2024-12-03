import os
from typing import Callable
from logging_config import get_logger


logger = get_logger(__name__)


class FileShifter:
    """
    Utility for safely updating files with an atomic replacement strategy.
    """

    @staticmethod
    def shift_files(f_name: str, action: Callable[[str], None], binary_mode: bool = False) -> int:
        """
        Update a file atomically by writing to a temporary file.

        Args:
            f_name: Path to the file to update.
            action: Function to update the file.
            binary_mode: Open file in binary mode if True.

        Returns:
            0 if successful, non-zero error code otherwise.
        """
        had_error = 0
        tmp_file = f_name + ".tmp"

        try:
            # Read original content
            with open(f_name, 'rb' if binary_mode else 'r') as orig:
                original_content = orig.read()

            mode = "wb" if binary_mode else "w"
            with open(tmp_file, mode) as f:
                # Capture the state before and after action
                before_pos = f.tell()
                action(f)
                after_pos = f.tell()

            # If no changes were made (file not modified), remove tmp and keep original
            if before_pos == after_pos:
                os.unlink(tmp_file)
                return 0

            # Check content of temporary file
            with open(tmp_file, 'rb' if binary_mode else 'r') as tmp:
                new_content = tmp.read()

            # If new content is empty, keep original file
            if not new_content:
                os.unlink(tmp_file)
                return 0

            # Replace the original file
            os.replace(tmp_file, f_name)
        except PermissionError:
            had_error = -2
            logger.error("Permission denied")
        except FileNotFoundError:
            had_error = -3
            logger.error("File not found")
        except OSError as e:
            # Handle cases where errno might be None
            had_error = e.errno * 2 if e.errno and e.errno == 2 else \
                e.errno * 3 if e.errno and e.errno == 3 else \
                    e.errno * 4 if e.errno else -1
            logger.error(f"OS error: {e}")
        except Exception as e:
            had_error = -1
            logger.error(f"Unexpected error: {e}")

        # Remove temporary file if it exists
        if os.path.exists(tmp_file):
            os.unlink(tmp_file)

        return had_error