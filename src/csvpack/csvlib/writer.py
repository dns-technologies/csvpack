from collections.abc import Generator, Iterable
from io import BufferedWriter
from typing import Any

from .core import RustCsvWriter
from ..common.repr import csvlib_repr


class CSVWriter:
    """CSV dump writer with lazy iterator."""

    metadata: list[dict[str, str]]
    fileobj: BufferedWriter | None
    delimiter: str
    quote_char: str
    encoding: str
    has_header: bool
    _writer: RustCsvWriter

    def __init__(
        self,
        metadata: list[dict[str, str]] | None = None,
        delimiter: str = ",",
        quote_char: str = '"',
        encoding: str = "utf-8",
        has_header: bool = True,
        fileobj: BufferedWriter | None = None,
    ) -> None:
        """Class initialization."""

        self.metadata = metadata or []
        self.delimiter = delimiter
        self.quote_char = quote_char
        self.encoding = encoding
        self.has_header = has_header
        self.fileobj = fileobj
        self._writer = RustCsvWriter(
            metadata=self.metadata,
            has_header=self.has_header,
            delimiter=self.delimiter,
            quote_char=self.quote_char,
            encoding=self.encoding,
        )

    @property
    def columns(self) -> list[str]:
        """Get column list."""

        return [col for dct in self.metadata for col, _ in dct.items()]

    @property
    def dtypes(self) -> list[str]:
        """Get data type list."""

        return [dtype for dct in self.metadata for _, dtype in dct.items()]

    @property
    def num_columns(self) -> int:
        """Get number of columns."""
        return len(self.metadata)

    @property
    def num_rows(self) -> int:
        """Get number of rows read so far."""

        return self._writer.row_count()

    def from_rows(
        self,
        rows: Iterable[list[Any] | tuple[Any, ...]],
    ) -> Generator[bytes, None, None]:
        """Write all rows lazily."""

        self._writer.feed_data(rows)

        for chunk in self._writer:
            yield chunk

    def write(self, rows: Iterable[list[Any] | tuple[Any, ...]]) -> None:
        """Write all rows into file."""

        if self.fileobj is None:
            raise ValueError("File object not defined!")

        for chunk in self.from_rows(rows):
            self.fileobj.write(chunk)

    def tell(self) -> int:
        """Return current position."""

        return self._writer.tell()

    def close(self) -> None:
        """Close file object."""

        if self.fileobj and hasattr(self.fileobj, "close"):
            self.fileobj.close()

    def __repr__(self) -> str:
        """String representation of CSVWriter."""

        return csvlib_repr(
            self.columns,
            self.dtypes,
            self.num_columns,
            self.num_rows,
            "writer",
        )
