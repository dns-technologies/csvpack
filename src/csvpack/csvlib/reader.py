from collections.abc import Generator
from io import BufferedReader
from typing import Any

from pandas import DataFrame as PdFrame
from polars import (
    DataFrame as PlFrame,
    LazyFrame as LfFrame,
    Object,
)

from .core import RustCsvReader
from ..common import ptype as PType
from ..common.dtype import PD_TYPE
from ..common.repr import csvlib_repr
from ..common.sizes import CHUNK_SIZE


ISLAZY = {
    False: PlFrame,
    True: LfFrame,
}


class CSVReader:
    """CSV dump reader with lazy iterator."""

    fileobj: BufferedReader
    metadata: list[dict[str, str]]
    delimiter: str
    quote_char: str
    encoding: str
    has_header: bool
    schema_overrides: dict[str, Object]
    pandas_astype: dict[str, str]
    _reader: RustCsvReader

    def __init__(
        self,
        fileobj: BufferedReader,
        metadata: list[dict[str, str]] | None = None,
        delimiter: str = ",",
        quote_char: str = '"',
        encoding: str = "utf-8",
        has_header: bool = True,
    ) -> None:
        """Class initialization."""

        self.fileobj = fileobj
        self.delimiter = delimiter
        self.quote_char = quote_char
        self.encoding = encoding
        self.has_header = has_header
        self.metadata = metadata or []
        self.schema_overrides = {
            column: Object
            for columns in self.metadata
            for column, ptype in columns.items()
            if PType.LIST in ptype
        }
        self.pandas_astype = {
            column: PD_TYPE.get(ptype)
            for columns in self.metadata
            for column, ptype in columns.items()
        }
        self._reader = RustCsvReader(
            fileobj=self.fileobj,
            metadata=self.metadata,
            has_header=self.has_header,
            delimiter=self.delimiter,
            quote_char=self.quote_char,
            encoding=self.encoding,
        )

    def __iter__(self) -> Generator[tuple[Any, ...], None, None]:
        """Lazy iterator over rows."""

        return self._reader

    def __next__(self) -> tuple[Any, ...]:
        """Get next row as tuple."""

        return next(self._reader)

    @property
    def columns(self) -> list[str]:
        """Get column list."""

        if self.metadata:
            return [col for dct in self.metadata for col, _ in dct.items()]

        return self._reader.get_headers()

    @property
    def dtypes(self) -> list[str]:
        """Get data type list."""

        return [dtype for dct in self.metadata for _, dtype in dct.items()]

    @property
    def num_columns(self) -> int:
        """Get number of columns."""

        return len(self.columns)

    @property
    def num_rows(self) -> int:
        """Get number of rows read so far."""

        return self._reader.row_count()

    def read_info(self) -> None:
        """Read info without reading data."""

        for _ in self._reader:
            continue

    def read_row(self) -> Generator[list[Any], None, None]:
        """Read single row."""

        yield self._reader.__next__()

    def to_rows(self) -> Generator[list[list[Any]], None, None]:
        """Read all rows."""

        for row in self._reader:
            yield row

    def to_pandas(self) -> PdFrame:
        """Convert to pandas.DataFrame."""

        return PdFrame(
            data=self.to_rows(),
            columns=self.columns,
        ).astype(self.pandas_astype)

    def to_polars(self, is_lazy: bool = False) -> PlFrame | LfFrame:
        """Convert to polars.DataFrame."""

        return ISLAZY[is_lazy](
            data=self.to_rows(),
            schema=self.columns,
            schema_overrides=self.schema_overrides,
            infer_schema_length=None,
        )

    def to_bytes(self) -> Generator[bytes, None, None]:
        """Get raw unpacked csv data as bytes."""

        while chunk := self.fileobj.read(CHUNK_SIZE):
            yield chunk

    def tell(self) -> int:
        """Return current position."""

        return self._reader.tell() or self.fileobj.tell()

    def close(self) -> None:
        """Close file object."""

        self._reader.close()

    def __repr__(self) -> str:
        """String representation of CSVReader."""

        return csvlib_repr(
            self.columns,
            self.dtypes,
            self.num_columns,
            self.num_rows,
            "reader",
        )
