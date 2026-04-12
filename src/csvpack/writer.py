from collections.abc import Iterable
from io import BufferedWriter
from struct import pack
from typing import Any
from zlib import (
    crc32,
    compress,
)

from light_compressor import (
    CompressionLevel,
    CompressionMethod,
    CompressorType,
)
from pandas import DataFrame as PdFrame
from polars import (
    DataFrame as PlFrame,
    LazyFrame as LfFrame,
)

from .common import (
    errors as Error,
    signatures as Signature,
    sizes as Size,
    struct_formats as Fmt,
)
from .common.metadata import (
    CSVPackMeta,
    metadata_from_frame,
)
from .common.repr import csvpack_repr
from .csvlib import CSVWriter


class CSVPackWriter:
    """Class for write CSVPack format."""

    metadata: CSVPackMeta | None
    fileobj: BufferedWriter | None
    compressed_length: int
    data_length: int
    compression_method: CompressionMethod
    compression_level: int
    s3_file: bool
    _writer: CSVWriter | None
    _writer_pos: int

    def __init__(
        self,
        metadata: bytes | None = None,
        fileobj: BufferedWriter | None = None,
        compression_method: CompressionMethod = CompressionMethod.ZSTD,
        compression_level: int = CompressionLevel.ZSTD_DEFAULT,
        s3_file: bool = False,
    ) -> None:
        """Class initialization."""

        self.metadata = metadata
        self.fileobj = fileobj
        self.compressed_length = Size.SEEK_SET
        self.data_length = -Size.SEEK_CUR
        self.compression_method = compression_method
        self.compression_level = compression_level
        self.s3_file = s3_file
        self._writer = None

        if self.fileobj:
            self._writer_pos = self.fileobj.tell()
        else:
            self._writer_pos = 0

        if self.metadata:
            self.init_metadata(self.metadata)

    @property
    def columns(self) -> list[str]:
        """Get column names."""

        if not self._writer:
            return []

        return self._writer.columns

    @property
    def dtypes(self) -> list[str]:
        """Get column data types."""

        if not self._writer:
            return []

        return self._writer.dtypes

    @property
    def num_columns(self) -> int:
        """Get number of columns."""

        return len(self.columns)

    @property
    def num_rows(self) -> int:
        """Get number of rows read so far."""

        return self._writer.num_rows

    def __validate_write_state(self) -> None:
        """Validate expected parameters."""

        if not self.fileobj:
            raise Error.CSVPackValueError("Fileobject not define.")
        if not self.fileobj.writable():
            raise Error.CSVPackModeError("Fileobject don't support write.")
        if not self.metadata:
            raise Error.CSVPackMetadataError("Metadata not defined.")

    def __get_compressor(self) -> CompressorType | None:
        """Get current compressor."""

        if self.compression_method is CompressionMethod.NONE:
            return None
        if isinstance(self.compression_method, CompressionMethod):
            return self.compression_method.compressor(self.compression_level)
        raise Error.CSVPackTypeError(
            f"Unsupported compression method {self.compression_method}"
        )

    def __write_header(self) -> None:
        """Write CSVPack header."""

        if not self._writer:
            self.init_metadata(self.metadata)

        self._writer_pos = self.fileobj.tell()

        metadata_bytes = bytes(self.metadata)
        metadata_zlib = compress(metadata_bytes)
        metadata_crc = pack(Fmt.U_LONG, crc32(metadata_zlib))
        metadata_length = pack(Fmt.U_LONG, len(metadata_zlib))
        compression_method = pack(Fmt.U_CHAR, self.compression_method.value)
        s3_marker = Signature.S3_FILE if self.s3_file else bytes(Size.S3_TAIL)

        for data in (
            Signature.HEADER,
            metadata_crc,
            metadata_length,
            metadata_zlib,
            compression_method,
            s3_marker,
        ):
            self._writer_pos += self.fileobj.write(data)

    def __write_data(self, bytes_data: Iterable[bytes]) -> None:
        """Write CSV data."""

        compressor = self.__get_compressor()
        start_pos = self.fileobj.tell()

        if compressor:
            for chunk in compressor.send_chunks(bytes_data):
                self.fileobj.write(chunk)
            self.data_length = compressor.decompressed_size
        else:
            for chunk in bytes_data:
                self.fileobj.write(chunk)
            self.data_length = self.fileobj.tell() - start_pos

        self.compressed_length = self.fileobj.tell() - self._writer_pos

    def __write_trailer(self) -> None:
        """Write compress length and data length."""

        if not self.s3_file:
            self.fileobj.seek(self._writer_pos - Size.S3_TAIL)

        self.fileobj.write(
            pack(
                Fmt.COMPRESS_LENGTH,
                self.compressed_length,
                self.data_length,
            )
        )

    def init_metadata(
        self,
        metadata: bytes | CSVPackMeta,
    ) -> None:
        """Initialize CSVWriter from metadata."""

        if isinstance(metadata, CSVPackMeta):
            self.metadata = metadata
        elif isinstance(metadata, bytes):
            self.metadata = CSVPackMeta.from_bytes(metadata)
        else:
            raise Error.CSVPackMetadataError("Metadata object error.")

        self._writer = CSVWriter(
            self.metadata.csv_metadata,
            self.metadata.delimiter,
            self.metadata.quote_char,
            self.metadata.encoding,
            self.metadata.has_header,
            self.fileobj,
        )

    def from_rows(
        self,
        rows: Iterable[list[Any] | tuple[Any, ...]],
    ) -> int:
        """Convert Python rows to CSVPack format."""

        if not self.metadata:
            raise Error.CSVPackMetadataError("Metadata not defined.")

        return self.from_bytes(self._writer.from_rows(rows))

    def from_pandas(
        self,
        data_frame: PdFrame,
    ) -> int:
        """Convert pandas.DataFrame to CSVPack format."""

        if not self.metadata:
            self.init_metadata(metadata_from_frame(data_frame))

        return self.from_rows(data_frame.itertuples(index=False))

    def from_polars(
        self,
        data_frame: PlFrame | LfFrame,
    ) -> int:
        """Convert polars.DataFrame to CSVPack format."""

        if data_frame.__class__ is LfFrame:
            data_frame = data_frame.collect(engine="streaming")

        if not self.metadata:
            self.init_metadata(metadata_from_frame(data_frame))

        return self.from_rows(data_frame.iter_rows())

    def from_bytes(
        self,
        bytes_data: Iterable[bytes],
    ) -> int:
        """Write CSV bytes into CSVPack format."""

        self.__validate_write_state()
        self.__write_header()
        self.__write_data(bytes_data)
        self.__write_trailer()
        self.fileobj.flush()
        return self.tell()

    def tell(self) -> int:
        """Return current position."""

        return self._writer.tell() or self.data_length

    def close(self) -> None:
        """Close file object."""

        self._writer.close()

    def __repr__(self) -> str:
        """String representation of CSVPackWriter."""

        return csvpack_repr(
            self.columns,
            self.dtypes,
            self.s3_file,
            self.compressed_length,
            self.data_length,
            self.compression_method,
            self.metadata,
        )
