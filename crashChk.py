class CrashChk:
    def __init__(self):
        self.last_status = ""
        try:
            with open("status.txt", "r") as f:
                self.last_status = f.readline().strip()
        except FileNotFoundError:
            try:
                with open("status.tmp", "r") as f:
                    self.last_status = f.readline().strip()
            except FileNotFoundError:
                pass

    def get_last_status(self) -> str:
        return self.last_status


if __name__ == '__main__':
    # Test the CrashChk functionality
    checker = CrashChk()
    print(f"Last status: {checker.get_last_status()}")