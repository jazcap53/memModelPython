import pytest
from arrBit import ArrBit, ArrBitSizeError
from ajTypes import u32Const

@pytest.fixture
def arr_bit():
    return ArrBit(array_size=4, bitset_size=8)

def test_set(arr_bit):
    arr_bit.set(5)
    assert arr_bit.test(5)

def test_reset(arr_bit):
    arr_bit.set(10)
    assert arr_bit.test(10)
    arr_bit.reset(10)
    assert not arr_bit.test(10)

def test_set_all(arr_bit):
    arr_bit.set()
    assert arr_bit.all()

def test_reset_all(arr_bit):
    arr_bit.set()
    arr_bit.reset()
    assert arr_bit.none()

def test_size(arr_bit):
    assert arr_bit.size() == 4 * 8

def test_count(arr_bit):
    arr_bit.set(5)
    arr_bit.set(10)
    assert arr_bit.count() == 2

def test_all(arr_bit):
    arr_bit.set()
    assert arr_bit.all()

def test_any(arr_bit):
    arr_bit.set(5)
    assert arr_bit.any()

def test_none(arr_bit):
    assert arr_bit.none()

def test_flip(arr_bit):
    arr_bit.set(5)
    arr_bit.flip(5)
    assert not arr_bit.test(5)

def test_flip_all(arr_bit):
    arr_bit.flip()
    assert arr_bit.all()

def test_or(arr_bit):
    other_arr_bit = ArrBit(array_size=4, bitset_size=8)
    other_arr_bit.set(0)
    other_arr_bit.set(31)
    arr_bit |= other_arr_bit
    assert arr_bit.test(0)
    assert arr_bit.test(31)

def test_from_bytes(arr_bit):
    arr_bit.set(5)
    arr_bit.set(10)
    bytes_data = arr_bit.to_bytes()
    new_arr_bit = ArrBit.from_bytes(bytes_data, array_size=4, bitset_size=8)
    assert new_arr_bit.test(5)
    assert new_arr_bit.test(10)


def test_to_bytes(arr_bit):
    arr_bit.set(5)
    arr_bit.set(10)
    bytes_data = arr_bit.to_bytes()
    expected_bits = [0, 0, 1, 0, 0, 0, 0, 0,  # 0x20: bit 5 set
                    0, 0, 0, 0, 0, 1, 0, 0,  # 0x04: bit 10 set
                    0, 0, 0, 0, 0, 0, 0, 0,  # 0x00
                    0, 0, 0, 0, 0, 0, 0, 0]  # 0x00
    expected_bytes = bytes(int(''.join(map(str, expected_bits[i:i+8])), 2) for i in range(0, len(expected_bits), 8))
    assert bytes_data == expected_bytes, f"Got {bytes_data.hex(' ')} expected {expected_bytes.hex(' ')}"

def test_invalid_size_from_bytes(arr_bit):
    with pytest.raises(ValueError):
        bytes_data = bytearray.fromhex('ff ff')
        ArrBit.from_bytes(bytes_data, array_size=1, bitset_size=8)

def test_invalid_size_to_bytes(arr_bit):
    with pytest.raises(IndexError):
        arr_bit.set(64)
        arr_bit.to_bytes()

def test_init_with_valid_size():
    # Test that the ArrBit constructor succeeds with valid size arguments
    valid_array_size = 1024
    valid_bitset_size = 1024
    arr_bit = ArrBit(array_size=valid_array_size, bitset_size=valid_bitset_size)
    assert isinstance(arr_bit, ArrBit)

def test_init_with_large_size():
    # Test that the ArrBit constructor raises ArrBitSizeError with excessively large size arguments
    large_array_size = 1024 * 1024
    large_bitset_size = 1024
    with pytest.raises(ArrBitSizeError):
        ArrBit(array_size=large_array_size, bitset_size=large_bitset_size)

def test_init_with_invalid_size_combination():
    # Test that the ArrBit constructor raises ArrBitSizeError with a valid size but invalid size combination
    valid_array_size = 1024
    valid_bitset_size = 1024
    with pytest.raises(ArrBitSizeError):
        ArrBit(array_size=valid_array_size, bitset_size=valid_bitset_size * 1024)

def test_error_message_on_size_limit_exceeded():
    # Test that the ArrBitSizeError exception includes the expected error message
    large_array_size = 1024 * 1024
    large_bitset_size = 1024
    with pytest.raises(ArrBitSizeError) as exc_info:
        ArrBit(array_size=large_array_size, bitset_size=large_bitset_size)
    expected_message = f"Requested ArrBit size ({large_array_size * large_bitset_size} bits) exceeds the maximum allowed size ({1024 * 1024} bits)."
    assert str(exc_info.value) == expected_message


def test_non_multiple_of_8_size():
    """Test with total bits that aren't multiples of 8."""
    arr_bit = ArrBit(array_size=1, bitset_size=13)  # 13 bits total
    arr_bit.set()
    assert arr_bit.count() == 13
    assert arr_bit.all()
    arr_bit.reset()
    assert arr_bit.none()

    # Test last bit
    arr_bit.set(12)
    assert arr_bit.test(12)
    assert not arr_bit.test(11)


def test_operation_combinations():
    """Test combinations of set, reset, flip on same bits."""
    arr_bit = ArrBit(array_size=1, bitset_size=8)
    arr_bit.set(3)
    arr_bit.set(3)  # Set again
    assert arr_bit.test(3)

    arr_bit.flip(3)
    assert not arr_bit.test(3)

    arr_bit.reset(3)
    assert not arr_bit.test(3)

    arr_bit.flip(3)
    assert arr_bit.test(3)


def test_or_after_operations():
    """Test OR operation after various other operations."""
    arr_bit1 = ArrBit(array_size=1, bitset_size=8)
    arr_bit2 = ArrBit(array_size=1, bitset_size=8)

    arr_bit1.set(1)
    arr_bit1.set(3)
    arr_bit1.reset(1)

    arr_bit2.set(2)
    arr_bit2.set(3)
    arr_bit2.flip(2)  # Now bit 2 is 0

    arr_bit1 |= arr_bit2
    assert not arr_bit1.test(1)
    assert not arr_bit1.test(2)
    assert arr_bit1.test(3)


def test_or_mismatched_sizes():
    """Test OR operation with mismatched sizes."""
    arr_bit1 = ArrBit(array_size=1, bitset_size=8)
    arr_bit2 = ArrBit(array_size=1, bitset_size=16)

    with pytest.raises(ValueError):
        arr_bit1 |= arr_bit2


def test_index_out_of_range():
    """Test index out of range for various operations."""
    arr_bit = ArrBit(array_size=1, bitset_size=8)

    with pytest.raises(IndexError):
        arr_bit.set(8)

    with pytest.raises(IndexError):
        arr_bit.reset(8)

    with pytest.raises(IndexError):
        arr_bit.test(8)

    with pytest.raises(IndexError):
        arr_bit.flip(8)


def test_minimum_size():
    """Test with minimum size (1,1)."""
    arr_bit = ArrBit(array_size=1, bitset_size=1)
    assert arr_bit.size() == 1

    arr_bit.set(0)
    assert arr_bit.test(0)
    assert arr_bit.all()

    arr_bit.reset(0)
    assert arr_bit.none()


def test_maximum_size():
    """Test with exactly maximum size."""
    max_bits = ArrBit.MAX_SIZE_LIMIT
    arr_bit = ArrBit(array_size=1, bitset_size=max_bits)
    assert arr_bit.size() == max_bits

    # Test first and last bit
    arr_bit.set(0)
    arr_bit.set(max_bits - 1)
    assert arr_bit.test(0)
    assert arr_bit.test(max_bits - 1)
    assert arr_bit.count() == 2


@pytest.mark.parametrize("array_size,bitset_size,expected_message", [
    (0, 0, "Both array_size and bitset_size must be positive"),
    (0, 1, "Both array_size and bitset_size must be positive"),
    (1, 0, "Both array_size and bitset_size must be positive"),
    (-1, 1, "Both array_size and bitset_size must be positive"),
    (1, -1, "Both array_size and bitset_size must be positive"),
    (-1, -1, "Both array_size and bitset_size must be positive"),
    (-1, 0, "Both array_size and bitset_size must be positive"),
    (0, -1, "Both array_size and bitset_size must be positive"),
    (10, -5, "Both array_size and bitset_size must be positive"),
    (-5, 10, "Both array_size and bitset_size must be positive"),
])
def test_invalid_sizes_disallowed(array_size, bitset_size, expected_message):
    """Test that zero and negative sizes are disallowed for both array_size and bitset_size."""
    with pytest.raises(ValueError) as exc_info:
        ArrBit(array_size=array_size, bitset_size=bitset_size)
    assert expected_message in str(exc_info.value)

