from typing import TypeVar, Generic

T = TypeVar('T')
U = TypeVar('U')
V = TypeVar('V')


class ArrBitSizeError(Exception):
    """Custom exception raised when the requested ArrBit size is too large."""
    pass


class ArrBit:
    """
    A class representing a bit array with efficient storage and operations.

    This class provides a bit array implementation that allows for setting,
    resetting, and testing individual bits, as well as performing operations
    on the entire bit array. It uses a single contiguous byte array for
    storage, which provides better performance and memory efficiency compared
    to a 2D array implementation.

    The bit array is conceptually divided into 'array_size' number of 'bitset_size'
    bit segments, but is stored and manipulated as a single continuous sequence of bits.

    Attributes:
        MAX_SIZE_LIMIT (int): Maximum allowed size for the bit array in bits.
        array_size (int): Number of bitset segments.
        bitset_size (int): Size of each bitset segment in bits.
        total_bits (int): Total number of bits in the array.
        bytes (bytearray): Internal storage for the bits.
    """

    MAX_SIZE_LIMIT = 1024 * 1024  # 1 MiB, adjust as needed

    def __init__(self, array_size: int, bitset_size: int):
        """
        Initialize the ArrBit instance.

        Args:
            array_size (int): Number of bitset segments.
            bitset_size (int): Size of each bitset segment in bits.

        Raises:
            ArrBitSizeError: If the requested size exceeds MAX_SIZE_LIMIT.
            ValueError: If array_size or bitset_size is not positive.
        """
        if array_size <= 0 or bitset_size <= 0:
            raise ValueError("Both array_size and bitset_size must be positive")

        requested_size = array_size * bitset_size
        if requested_size > self.MAX_SIZE_LIMIT:
            raise ArrBitSizeError(
                f"Requested ArrBit size ({requested_size} bits) exceeds the maximum allowed size ({self.MAX_SIZE_LIMIT} bits)."
            )

        self.array_size = array_size
        self.bitset_size = bitset_size
        self.total_bits = array_size * bitset_size
        self.bytes = bytearray((self.total_bits + 7) // 8)

    def test(self, ix: int) -> bool:
        """
        Test if a specific bit is set.

        Args:
            ix (int): The index of the bit to test.

        Returns:
            bool: True if the bit is set, False otherwise.

        Raises:
            IndexError: If the index is out of range.
        """
        if 0 <= ix < self.total_bits:
            byte_index = ix // 8
            bit_in_byte = ix % 8
            return bool(self.bytes[byte_index] & (1 << bit_in_byte))
        raise IndexError(f"Bit index {ix} is out of range for ArrBit of size {self.total_bits}")

    def set(self, ix: int = None):
        """
        Set a specific bit or all bits to 1.

        Args:
            ix (int, optional): The index of the bit to set. If None, sets all bits.

        Raises:
            IndexError: If the index is out of range.
        """
        if ix is None:
            self.bytes = bytearray(b'\xff' * len(self.bytes))
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
        """
        Reset a specific bit or all bits to 0.

        Args:
            ix (int, optional): The index of the bit to reset. If None, resets all bits.

        Raises:
            IndexError: If the index is out of range.
        """
        if ix is None:
            self.bytes = bytearray(len(self.bytes))
        else:
            if 0 <= ix < self.total_bits:
                byte_index = ix // 8
                bit_in_byte = ix % 8
                self.bytes[byte_index] &= ~(1 << bit_in_byte)
            else:
                raise IndexError(f"Bit index {ix} is out of range for ArrBit of size {self.total_bits}")

    def size(self) -> int:
        """
        Get the total number of bits in the array.

        Returns:
            int: The total number of bits.
        """
        return self.total_bits

    def count(self) -> int:
        """
        Count the number of set bits (1s) in the array.

        Returns:
            int: The number of set bits.
        """
        return sum(bin(byte).count('1') for byte in self.bytes)

    def all(self) -> bool:
        """
        Check if all bits in the array are set to 1.

        Returns:
            bool: True if all bits are set, False otherwise.
        """
        if len(self.bytes) > 0:
            if any(byte != 0xFF for byte in self.bytes[:-1]):
                return False
            last_byte_mask = (1 << (self.total_bits % 8 or 8)) - 1
            return self.bytes[-1] == (last_byte_mask & 0xFF)
        return True

    def any(self) -> bool:
        """
        Check if any bit in the array is set to 1.

        Returns:
            bool: True if any bit is set, False otherwise.
        """
        return any(self.bytes)

    def none(self) -> bool:
        """
        Check if no bits in the array are set to 1.

        Returns:
            bool: True if no bits are set, False otherwise.
        """
        return all(byte == 0 for byte in self.bytes)

    def flip(self, ix: int = None):
        """
        Flip a specific bit or all bits (0 becomes 1 and vice versa).

        Args:
            ix (int, optional): The index of the bit to flip. If None, flips all bits.

        Raises:
            IndexError: If the index is out of range.
        """
        if ix is None:
            for i in range(len(self.bytes)):
                self.bytes[i] = ~self.bytes[i] & 0xFF
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
        """
        Perform bitwise OR operation with another ArrBit instance.

        Args:
            other (ArrBit): The other ArrBit instance to OR with.

        Returns:
            ArrBit: The result of the OR operation (self).

        Raises:
            ValueError: If the sizes of the two ArrBit instances don't match.
        """
        if self.total_bits != other.total_bits:
            raise ValueError("ArrBit sizes must match for bitwise OR")

        for i in range(len(self.bytes)):
            self.bytes[i] |= other.bytes[i]

        return self

    @classmethod
    def from_bytes(cls, bytes_data: bytes, array_size: int, bitset_size: int) -> 'ArrBit':
        """
        Create an ArrBit instance from a bytes object.

        Args:
            bytes_data (bytes): The bytes object to create the ArrBit from.
            array_size (int): Number of bitset segments.
            bitset_size (int): Size of each bitset segment in bits.

        Returns:
            ArrBit: A new ArrBit instance.

        Raises:
            ValueError: If the input bytes size doesn't match the expected size.
        """
        expected_size = (array_size * bitset_size + 7) // 8
        if len(bytes_data) != expected_size:
            raise ValueError(f"Input bytes size {len(bytes_data)} does not match expected size {expected_size}")

        arr_bit = cls(array_size, bitset_size)
        arr_bit.bytes = bytearray(bytes_data)

        return arr_bit

    def to_bytes(self) -> bytes:
        """
        Convert the ArrBit to a bytes object.

        Returns:
            bytes: The byte representation of the ArrBit.
        """
        return bytes(self.bytes)


if __name__ == '__main__':
    print("This module's tests have been moved to the PyTest suite.")
    print("Please run tests using the PyTest command:")
    print("    pytest tests/test_arrBit.py")