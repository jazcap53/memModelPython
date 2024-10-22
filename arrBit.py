from typing import TypeVar, Generic

T = TypeVar('T')
U = TypeVar('U')
V = TypeVar('V')


class ArrBitSizeError(Exception):
    """Custom exception raised when the requested ArrBit size is too large."""
    pass


class ArrBit(Generic[T, U, V]):
    def __init__(self, array_size: int, bitset_size: int):
        MAX_SIZE_LIMIT = 1024 * 1024  # 1 MiB, adjust as needed
        requested_size = array_size * bitset_size
        if requested_size > MAX_SIZE_LIMIT:
            raise ArrBitSizeError(
                f"Requested ArrBit size ({requested_size} bits) exceeds the maximum allowed size ({MAX_SIZE_LIMIT} bits)."
            )
        self.array_size = array_size
        self.bitset_size = bitset_size
        self.arBt = [[False] * bitset_size for _ in range(array_size)]

    def test(self, ix: V) -> bool:
        array_idx = ix // self.bitset_size
        bit_idx = ix % self.bitset_size
        return self.arBt[array_idx][bit_idx]

    def set(self, ix: V = None):
        if ix is None:
            for i in range(self.array_size):
                for j in range(self.bitset_size):
                    self.arBt[i][j] = True
        else:
            if 0 <= ix < self.size():
                array_idx = ix // self.bitset_size
                bit_idx = ix % self.bitset_size
                self.arBt[array_idx][bit_idx] = True
            else:
                raise IndexError(f"Bit index {ix} is out of range for ArrBit of size {self.size()}")

    def reset(self, ix: V = None):
        if ix is None:
            for i in range(self.array_size):
                for j in range(self.bitset_size):
                    self.arBt[i][j] = False
        else:
            self.arBt[ix // self.bitset_size][ix % self.bitset_size] = False

    def size(self) -> U:
        return self.array_size * self.bitset_size

    def count(self) -> V:
        return sum(sum(row) for row in self.arBt)

    def all(self) -> bool:
        return all(all(row) for row in self.arBt)

    def any(self) -> bool:
        return any(any(row) for row in self.arBt)

    def flip(self, ix: V = None):
        if ix is None:
            for i in range(self.array_size):
                for j in range(self.bitset_size):
                    self.arBt[i][j] = not self.arBt[i][j]
        else:
            i, j = ix // self.bitset_size, ix % self.bitset_size
            self.arBt[i][j] = not self.arBt[i][j]

    def none(self) -> bool:
        return not self.any()

    def __ior__(self, other: 'ArrBit[T, U, V]') -> 'ArrBit[T, U, V]':
        if self is not other:
            for i in range(self.array_size):
                for j in range(self.bitset_size):
                    if other.arBt[i][j]:
                        self.arBt[i][j] = True
        return self

    @classmethod
    def from_bytes(cls, bytes_data: bytes, array_size: int, bitset_size: int) -> 'ArrBit':
        # First validate the input size
        expected_size = (array_size * bitset_size + 7) // 8
        if len(bytes_data) != expected_size:
            raise ValueError(f"Input bytes size {len(bytes_data)} does not match expected size {expected_size}")

        arr_bit = cls(array_size, bitset_size)
        for i, byte in enumerate(bytes_data):
            for j in range(8):
                bit_index = i * 8 + j
                if bit_index >= arr_bit.size():
                    break
                if byte & (1 << j):
                    arr_bit.set(bit_index)
        return arr_bit

    def to_bytes(self) -> bytes:
        byte_count = (self.size() + 7) // 8  # Round up to nearest byte
        result = bytearray(byte_count)
        for bit_index in range(self.size()):
            if self.test(bit_index):
                byte_index = bit_index // 8  # Which byte this bit belongs to
                bit_in_byte = bit_index % 8  # Which position in that byte
                result[byte_index] |= (1 << bit_in_byte)  # Set the bit
        return bytes(result)


if __name__ == '__main__':
    # Create an ArrBit instance
    arr_bit = ArrBit(array_size=4, bitset_size=8)

    # Set some bits
    arr_bit.set(5)
    arr_bit.set(10)
    arr_bit.set(15)

    # Test some bits
    print(f"Bit 5 is set: {arr_bit.test(5)}")
    print(f"Bit 7 is set: {arr_bit.test(7)}")

    # Count set bits
    print(f"Number of set bits: {arr_bit.count()}")

    # Check if any bit is set
    print(f"Any bit set: {arr_bit.any()}")

    # Reset a bit
    arr_bit.reset(10)

    # Flip all bits
    arr_bit.flip()

    # Check if all bits are set
    print(f"All bits set after flip: {arr_bit.all()}")

    # Create another ArrBit instance
    other_arr_bit = ArrBit(array_size=4, bitset_size=8)
    other_arr_bit.set(0)
    other_arr_bit.set(31)

    # Perform OR operation
    arr_bit |= other_arr_bit

    # Check the result
    print(f"Bit 0 is set after OR: {arr_bit.test(0)}")
    print(f"Bit 31 is set after OR: {arr_bit.test(31)}")