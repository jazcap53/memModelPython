import os
from typing import Callable


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
                action(f)

            # Check if the action actually modified the file
            with open(tmp_file, 'rb' if binary_mode else 'r') as tmp:
                new_content = tmp.read()

            # If no changes, just remove tmp file
            if new_content == original_content:
                os.unlink(tmp_file)
                return 0

            os.replace(tmp_file, f_name)
        except PermissionError:
            had_error = -2
            print(f"ERROR: Permission denied in {__file__}")
        except FileNotFoundError:
            had_error = -3
            print(f"ERROR: File not found in {__file__}")
        except OSError as e:
            # Handle cases where errno might be None
            had_error = e.errno * 2 if e.errno and e.errno == 2 else \
                e.errno * 3 if e.errno and e.errno == 3 else \
                    e.errno * 4 if e.errno else -1
            print(f"ERROR: OS error {e} in {__file__}")
        except Exception as e:
            had_error = -1
            print(f"Unexpected error: {e} in {__file__}")

        # Remove temporary file if it exists
        if os.path.exists(tmp_file):
            os.unlink(tmp_file)

        return had_error