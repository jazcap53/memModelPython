# cleanup.py
import os


def cleanup():
    files_to_remove = [
        'disk_file.bin',
        'status.txt',
        'jrnl_file.bin',
        'node_file.bin',
        'free_file.bin',
        'output.txt'
    ]

    for file in files_to_remove:
        if os.path.exists(file):
            os.remove(file)
            print(f"Removed {file}")


if __name__ == "__main__":
    cleanup()