# cleanup.py
import os
import glob
from typing import List


def get_files_to_remove() -> List[str]:
    """
    Create and return a list of files to remove.

    Returns:
        List of filenames (strings) to remove, combining both static entries
        and glob patterns.
    """
    # Static list of files to remove
    static_files = [
        'disk_file.bin',
        'status.txt',
        'jrnl_file.bin',
        'node_file.bin',
        'free_file.bin',
        'output.txt'
    ]

    # Use glob to find files matching patterns
    glob_patterns = [
        '*_jrnl'  # Files ending in '_jrnl'
    ]

    # Create list of files from glob patterns
    glob_files = []
    for pattern in glob_patterns:
        glob_files.extend(glob.glob(pattern))

    # Combine static and globbed files, removing duplicates
    all_files = list(set(static_files + glob_files))

    return all_files


def cleanup(files_to_remove: List[str] = None) -> None:
    """
    Remove specified files if they exist.

    Args:
        files_to_remove: List of files to remove. If None, get list from
                        get_files_to_remove().
    """
    if files_to_remove is None:
        files_to_remove = get_files_to_remove()

    for file in files_to_remove:
        if os.path.exists(file):
            try:
                os.remove(file)
                print(f"Removed {file}")
            except OSError as e:
                print(f"Error removing {file}: {e}")
        else:
            print(f"File not found: {file}")


if __name__ == "__main__":
    cleanup()