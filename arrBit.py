from typing import TypeVar, Generic

T = TypeVar('T')
U = TypeVar('U')
V = TypeVar('V')


class ArrBitSizeError(Exception):
    """Custom exception raised when the requested ArrBit size is too large."""
    pass


class ArrBit:
    MAX_SIZE_LIMIT = 1024 * 1024  # 1 MiB, adjust as needed

    def __init__(self, array_size: int, bitset_size: int):
        # Validate size constraints
        requested_size = array_size * bitset_size
        if requested_size > self.MAX_SIZE_LIMIT:
            raise ArrBitSizeError(
                f"Requested ArrBit size ({requested_size} bits) exceeds the maximum allowed size ({self.MAX_SIZE_LIMIT} bits)."
            )

        self.array_size = array_size
        self.bitset_size = bitset_size

        # Total number of bits
        self.total_bits = array_size * bitset_size

        # Use a single byte array for storage
        self.bytes = bytearray((self.total_bits + 7) // 8)

    def test(self, ix: int) -> bool:
        if 0 <= ix < self.total_bits:
            byte_index = ix // 8
            bit_in_byte = ix % 8
            return bool(self.bytes[byte_index] & (1 << bit_in_byte))
        raise IndexError(f"Bit index {ix} is out of range for ArrBit of size {self.total_bits}")

    def set(self, ix: int = None):
        if ix is None:
            # Set all bits
            self.bytes = bytearray(b'\xff' * len(self.bytes))
            # Clear any extra bits in the last byte
            if self.total_bits % 8 != 0:
                last_byte_mask = (1 << (self.total_bits % 8)) - 1
                self.bytes[-1] &= last_byte_mask
        else:
            if 0 <= ix < self.total_bits:
                byte_index = ix // 8
                bit_in_byte = ix % 8
                self.bytes[byte_index] |= (1 << bit_in_byte)
            else:
                raise IndexError(f"Bit index {ix} is out of range for ArrBit of size {self.total_bits}")

    def reset(self, ix: int = None):
        if ix is None:
            # Reset all bits
            self.bytes = bytearray(len(self.bytes))
        else:
            if 0 <= ix < self.total_bits:
                byte_index = ix // 8
                bit_in_byte = ix % 8
                self.bytes[byte_index] &= ~(1 << bit_in_byte)
            else:
                raise IndexError(f"Bit index {ix} is out of range for ArrBit of size {self.total_bits}")

    def size(self) -> int:
        return self.total_bits

    def count(self) -> int:
        return sum(bin(byte).count('1') for byte in self.bytes)

    def all(self) -> bool:
        # Check if all bits are set, handling potential extra bits in last byte
        if len(self.bytes) > 0:
            # Check full bytes
            if any(byte != 0xFF for byte in self.bytes[:-1]):
                return False

            # Check last byte, considering only the bits we actually use
            last_byte_mask = (1 << (self.total_bits % 8 or 8)) - 1
            return self.bytes[-1] == (last_byte_mask & 0xFF)
        return True

    def any(self) -> bool:
        return any(self.bytes)

    def none(self) -> bool:
        return all(byte == 0 for byte in self.bytes)

    def flip(self, ix: int = None):
        if ix is None:
            # Flip all bits
            for i in range(len(self.bytes)):
                self.bytes[i] = ~self.bytes[i] & 0xFF

            # Clear any extra bits in the last byte
            if self.total_bits % 8 != 0:
                last_byte_mask = (1 << (self.total_bits % 8)) - 1
                self.bytes[-1] &= last_byte_mask
        else:
            if 0 <= ix < self.total_bits:
                byte_index = ix // 8
                bit_in_byte = ix % 8
                self.bytes[byte_index] ^= (1 << bit_in_byte)
            else:
                raise IndexError(f"Bit index {ix} is out of range for ArrBit of size {self.total_bits}")

    def __ior__(self, other: 'ArrBit') -> 'ArrBit':
        # Ensure other ArrBit has the same total number of bits
        if self.total_bits != other.total_bits:
            raise ValueError("ArrBit sizes must match for bitwise OR")

        for i in range(len(self.bytes)):
            self.bytes[i] |= other.bytes[i]

        return self

    @classmethod
    def from_bytes(cls, bytes_data: bytes, array_size: int, bitset_size: int) -> 'ArrBit':
        # Validate input size
        expected_size = (array_size * bitset_size + 7) // 8
        if len(bytes_data) != expected_size:
            raise ValueError(f"Input bytes size {len(bytes_data)} does not match expected size {expected_size}")

        # Create ArrBit instance
        arr_bit = cls(array_size, bitset_size)
        arr_bit.bytes = bytearray(bytes_data)

        return arr_bit

    def to_bytes(self) -> bytes:
        return bytes(self.bytes)


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