class _ChangeLogHandler:
    def write_buffer_to_disk(self, is_end: bool = False) -> bool:
        """Coordinate writing buffered pages to disk.

        This method manages the high-level process of writing buffered pages to disk:
        1. Iterates through the buffer
        2. Verifies CRC for each page
        3. Delegates actual disk writing to Journal.write_block_to_disk

        This method owns the buffer and understands its structure, while
        delegating physical I/O operations to the Journal class.

        Args:
            is_end: Whether this is the final write operation in the current sequence.
                   When True, ensures all buffered pages are written.

        Returns:
            bool: True if all writes were successful, False otherwise

        See Also:
            Journal.write_block_to_disk: Handles the actual disk I/O for individual blocks
        """
        pass  # Implementation here


class Journal:
    def write_block_to_disk(self, block_num: bNum_t, page: Page):
        """Write a single block to disk.

        This method handles the low-level disk I/O for a single block:
        1. Seeks to the correct disk position
        2. Checks if the block is marked as dirty
        3. Performs the actual write operation

        This method is called by _ChangeLogHandler.write_buffer_to_disk() for each
        block that needs to be written. It handles the physical I/O details that
        the ChangeLogHandler doesn't need to know about.

        Args:
            block_num: The block number to write
            page: The page containing the data to write

        Raises:
            IOError: If the write operation fails

        See Also:
            _ChangeLogHandler.write_buffer_to_disk: Coordinates the overall buffer writing process
        """
        pass  # Implementation here