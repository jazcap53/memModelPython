import subprocess
import sys
from typing import List, Tuple
import argparse


def run_module(filename: str) -> Tuple[str, str]:
    """
    Run a Python module and return its stdout and stderr output.

    Args:
        filename: The name of the Python file to run

    Returns:
        Tuple of (stdout, stderr) as strings
    """
    try:
        result = subprocess.run(
            [sys.executable, filename],
            capture_output=True,
            text=True,
            timeout=10  # 10 second timeout
        )
        return result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return "", f"Timeout expired running {filename}"
    except subprocess.SubprocessError as e:
        return "", f"Error running {filename}: {str(e)}"
    except Exception as e:
        return "", f"Unexpected error running {filename}: {str(e)}"


def run_module_with_cleanup(filename: str) -> Tuple[str, str]:
    """Run cleanup and then run a module."""
    try:
        # First run cleanup
        subprocess.run([sys.executable, 'cleanup.py'],
                       check=True,
                       capture_output=True)

        # Then run the module
        result = subprocess.run(
            [sys.executable, filename],
            capture_output=True,
            text=True,
            timeout=10
        )
        return result.stdout, result.stderr
    except subprocess.SubprocessError as e:
        return "", f"Error in {filename}: {str(e)}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--no-cleanup', action='store_true',
                        help='Skip cleanup between tests')
    args = parser.parse_args()

    # List of files to run
    files_to_run = [
        'ajTypes.py',
        'ajUtils.py',
        'arrBit.py',
        'ajCrc.py',
        'crashChk.py',
        'fileShifter.py',
        'status.py',
        'wipeList.py',
        'myMemory.py',
        'pageTable.py',
        'inodeTable.py',
        'freeList.py',
        'change.py',
        'simDisk.py',
        'driver.py',
        'journal.py',
        'fileMan.py',
        'memMan.py',
        'client.py'
    ]

    run_func = run_module if args.no_cleanup else run_module_with_cleanup

    # Run each file and display output
    for filename in files_to_run:
        print(f"\nfile name: {filename}")
        print("\nnormal output:")

        stdout, stderr = run_func(filename)

        # Print stdout if it exists, otherwise indicate no output
        if stdout.strip():
            print(stdout.rstrip())  # rstrip() removes trailing whitespace
        else:
            print("(no output)")

        print("\nerror output:")
        # Print stderr if it exists, otherwise indicate no errors
        if stderr.strip():
            print(stderr.rstrip())

            if stderr.startswith("ERROR OUTPUT"):
                print("(this message is part of normal program flow)")
        else:
            print("(no errors)")

        print("\n" + "=" * 80)  # Print a separator line


if __name__ == "__main__":
    main()