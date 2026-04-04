from io import BufferedReader
from typing import Any, Iterator, Optional


class RustCsvReader:
    """High-performance CSV reader with buffering (8KB).
    Returns rows as tuples. Implements Python iterator protocol.
    """

    def __init__(
        self,
        fileobj: BufferedReader,
        metadata: Optional[list[dict[str, str]]] = None,
        has_header: bool = True,
        delimiter: str = ",",
        quote_char: str = '"',
        encoding: str = "utf-8",
    ) -> None:
        """Initialize CSV reader iterator.

        Args:
            fileobj: File-like object to read from.
            metadata: Column metadata as list of {name: type} dicts.
            has_header: Whether the CSV has a header row.
            delimiter: Field delimiter character.
            quote_char: Quote character for fields.
            encoding: File encoding.
        """
        ...

    def __iter__(self) -> "RustCsvReader":
        """Return self as iterator."""
        ...

    def __next__(self) -> tuple[Any, ...]:
        """Return next row as tuple.

        Returns:
            Tuple of values with types converted according to metadata.

        Raises:
            StopIteration: When no more rows available.
            ValueError: On parsing error.
        """
        ...

    def tell(self) -> int:
        """Return current position in the file.

        Returns:
            Current file position in bytes (accounts for buffered data).
        """
        ...

    def close(self) -> None:
        """Close the underlying file object."""
        ...

    def row_count(self) -> int:
        """Get number of rows read so far.

        Returns:
            Number of rows returned by __next__ (excluding header).
        """
        ...

    def get_headers(self) -> list[str]:
        """Get column names from header row.

        Returns:
            List of column names if has_header=True, otherwise empty list.
        """
        ...


class RustCsvWriter:
    """High-performance CSV writer that returns bytes for each row.
    Does NOT write to file directly."""

    def __init__(
        self,
        metadata: Optional[list[dict[str, str]]] = None,
        has_header: bool = True,
        delimiter: str = ",",
        quote_char: str = '"',
        encoding: str = "utf-8",
    ) -> None:
        """Initialize CSV writer iterator.

        Args:
            metadata: Column metadata as list of {name: type} dicts.
            has_header: Whether to write header row.
            delimiter: Field delimiter character.
            quote_char: Quote character for fields.
            encoding: Output encoding.
        """
        ...

    def __iter__(self) -> "RustCsvWriter":
        """Return self as iterator."""
        ...

    def __next__(self) -> bytes:
        """Return next chunk of CSV data.

        Returns:
            Bytes chunk of CSV data (8KB-64KB).

        Raises:
            StopIteration: When all data has been yielded.
        """
        ...

    def feed_data(self, rows: Iterator[list[Any] | tuple[Any, ...]]) -> None:
        """Feed rows to the writer.

        Args:
            rows: Iterator of rows (each row is a list or tuple).
        """
        ...

    def tell(self) -> int:
        """Return total bytes written across all rows.

        Returns:
            Total number of bytes produced so far.
        """
        ...

    def row_count(self) -> int:
        """Get number of rows read so far.

        Returns:
            Number of rows returned by __next__ (excluding header).
        """
        ...
