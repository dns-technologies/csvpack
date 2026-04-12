from collections.abc import Generator
from io import BufferedReader
from struct import unpack
from typing import Any
from zlib import (
    crc32,
    decompress,
)

from light_compressor import (
    CompressionMethod,
    LimitedReader,
    define_reader,
)
from pandas import DataFrame as PdFrame
from polars import (
    DataFrame as PlFrame,
    LazyFrame as LfFrame,
    Object,
)

from .common import (
    errors as Error,
    ptype as PType,
    signatures as Signature,
    sizes as Size,
    struct_formats as Fmt,
)
from .common.metadata import CSVPackMeta
from .common.repr import csvpack_repr
from .csvlib import CSVReader

ISLAZY = {
    False: PlFrame,
    True: LfFrame,
}


class CSVPackReader:
    """Class for read CSVPack format."""

    fileobj: BufferedReader
    metadata: CSVPackMeta
    compressed_length: int
    data_length: int
    compression_method: CompressionMethod
    compression_stream: BufferedReader
    s3_file: bool
    schema_overrides: dict[str, Object]
    _reader: CSVReader
    _reader_pos: int

    def __init__(
        self,
        fileobj: BufferedReader,
    ) -> None:
        """Class initialization."""

        self.fileobj = fileobj
        header = self.fileobj.read(Size.HEADER_LENS)

        if header != Signature.HEADER:
            raise Error.CSVPackHeaderError()

        (
            metadata_crc,
            metadata_length,
        ) = unpack(
            Fmt.METADATA_CRC_LENGTH,
            self.fileobj.read(Size.METADATA_PROMPT),
        )
        metadata_zlib = self.fileobj.read(metadata_length)

        if crc32(metadata_zlib) != metadata_crc:
            raise Error.CSVPackMetadataCrcError()

        self.metadata = CSVPackMeta.from_bytes(decompress(metadata_zlib))
        (
            compression_method,
            self.compressed_length,
            self.data_length,
        ) = unpack(
            Fmt.COMPRESS_METHOD_LENGTH,
            self.fileobj.read(Size.CSVDATA_PROMPT),
        )
        self.compression_method = CompressionMethod(compression_method)
        self.s3_file = (
            self.compressed_length,
            self.data_length,
        ) == Signature.S3_FILE_INTEGERS
        self._reader_pos = self.fileobj.tell()

        if self.s3_file:
            self.fileobj.seek(-Size.S3_TAIL, Size.SEEK_END)
            limit = self.fileobj.tell()
            (
                self.compressed_length,
                self.data_length,
            ) = unpack(
                Fmt.COMPRESS_LENGTH,
                self.fileobj.read(Size.S3_TAIL)
            )
            self.fileobj.seek(self._reader_pos)
            self.fileobj = LimitedReader(self.fileobj, limit)

        self.compression_stream = define_reader(
            self.fileobj,
            self.compression_method,
        )
        self._reader = CSVReader(
            self.compression_stream,
            self.metadata.csv_metadata,
            self.metadata.delimiter,
            self.metadata.quote_char,
            self.metadata.encoding,
            self.metadata.has_header,
        )
        self.schema_overrides = {
            column: Object
            for columns in self.metadata.csv_metadata
            for column, ptype in columns.items()
            if PType.LIST in ptype
        }
        self._str = None

    @property
    def columns(self) -> list[str]:
        """Get column names."""

        return self._reader.columns

    @property
    def dtypes(self) -> list[str]:
        """Get column data types."""

        return self._reader.dtypes

    @property
    def num_columns(self) -> int:
        """Get number of columns."""

        return len(self.columns)

    @property
    def num_rows(self) -> int:
        """Get number of rows read so far."""

        return self._reader.num_rows

    def read_info(self) -> None:
        """Read info without reading data."""

        self._reader.read_info()

    def to_rows(self) -> Generator[list[Any], None, None]:
        """Convert to python objects."""

        return self._reader.to_rows()

    def to_pandas(self) -> PdFrame:
        """Convert to pandas.DataFrame."""

        return PdFrame(
            data=self._reader.to_rows(),
            columns=self._reader.columns,
        ).astype(self.metadata.pandas_astype)

    def to_polars(self, is_lazy: bool = False) -> PlFrame | LfFrame:
        """Convert to polars.DataFrame."""

        return ISLAZY[is_lazy](
            data=self._reader.to_rows(),
            schema=self._reader.columns,
            schema_overrides=self.schema_overrides,
            infer_schema_length=None,
        )

    def to_bytes(self) -> Generator[bytes, None, None]:
        """Get raw unpacked csv data as bytes."""

        if self.compression_method is CompressionMethod.NONE:
            self.compression_stream.seek(self._reader_pos)
        else:
            self.compression_stream.seek(Size.SEEK_SET)

        while chunk := self.compression_stream.read(Size.CHUNK_SIZE):
            yield chunk

    def tell(self) -> int:
        """Return current position."""

        return self._reader.tell()

    def close(self) -> None:
        """Close file object."""

        if hasattr(self.fileobj, "close"):
            self.fileobj.close()

    def __repr__(self) -> str:
        """String representation of CSVPackReader."""

        if not self._str:
            self._str = csvpack_repr(
                self.columns,
                self.dtypes,
                self.s3_file,
                self.compressed_length,
                self.data_length,
                self.compression_method,
                self.metadata,
            )

        return self._str
