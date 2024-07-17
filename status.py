import os
from fileShifter import FileShifter

class Status:
    def __init__(self, sfn: str):
        self.file_name = sfn
        self.shifter = FileShifter()

    def rd(self) -> str:
        try:
            with open(self.file_name, 'r') as f:
                return f.readline().strip()
        except IOError:
            return "ERROR: Can not open status file for read in Status.rd()."

    def wrt(self, msg: str) -> int:
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
