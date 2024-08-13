import struct
import os

def write_test_file(filename):
    with open(filename, 'wb') as f:
        # Test 64-bit value
        value_64 = 0x1234567890ABCDEF
        f.write(value_64.to_bytes(8, 'little'))
        f.write(struct.pack('<Q', value_64))
        os.write(f.fileno(), value_64.to_bytes(8, 'little'))

        # Test 32-bit value
        value_32 = 0x12345678
        f.write(value_32.to_bytes(4, 'little'))
        f.write(struct.pack('<I', value_32))
        os.write(f.fileno(), value_32.to_bytes(4, 'little'))

def read_test_file(filename):
    with open(filename, 'rb') as f:
        print("64-bit values:")
        for _ in range(3):
            print(f.read(8).hex())
        print("32-bit values:")
        for _ in range(3):
            print(f.read(4).hex())

write_test_file('endian_test.bin')
read_test_file('endian_test.bin')
