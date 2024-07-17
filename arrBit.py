from typing import TypeVar, Generic

T = TypeVar('T')
U = TypeVar('U')
V = TypeVar('V')

class ArrBit(Generic[T, U, V]):
    def __init__(self, array_size: int, bitset_size: int):
        self.array_size = array_size
        self.bitset_size = bitset_size
        self.arBt = [[False] * bitset_size for _ in range(array_size)]

    def test(self, ix: V) -> bool:
        return self.arBt[ix // self.bitset_size][ix % self.bitset_size]

    def set(self, ix: V = None):
        if ix is None:
            for i in range(self.array_size):
                for j in range(self.bitset_size):
                    self.arBt[i][j] = True
        else:
            self.arBt[ix // self.bitset_size][ix % self.bitset_size] = True

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