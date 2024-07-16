import os
from typing import Callable
from ajUtils import get_cur_time


class FileShifter:
    @staticmethod
    def shift_files(f_name: str, action: Callable[[str], None]) -> int:
        had_error = 0
        tmp_file = f_name + ".tmp"

        try:
            with open(tmp_file, "w") as f:
                action(f)

            os.unlink(f_name)
            os.link(tmp_file, f_name)
            os.unlink(tmp_file)
        except IOError:
            had_error = -1
        except OSError as e:
            had_error = e.errno * 2 if e.errno == 2 else e.errno * 3 if e.errno == 3 else e.errno * 4

        if had_error:
            print(f"ERROR: Error value of {had_error} in {__file__}, {__name__} at time {get_cur_time()}")

        return had_error


if __name__ == '__main__':
    # Test the FileShifter functionality
    def update_file(f):
        f.write("Updated content")

    result = FileShifter.shift_files("test_file.txt", update_file)
    print(f"File shift result: {result}")