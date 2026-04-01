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

    fileobj: BufferedWriter | None
    metadata: CSVPackMeta | None = None
    compressed_length: int
    data_length: int
    compression_method: CompressionMethod
    compression_level: int
    s3_file: bool
    csv_start: int | None = 0
    csv_writer: CSVWriter | None = None

    def __init__(
        self,
        fileobj: BufferedWriter | None = None,
        metadata: bytes | None = None,
        compression_method: CompressionMethod = CompressionMethod.ZSTD,
        compression_level: int = CompressionLevel.ZSTD_DEFAULT,
        s3_file: bool = False,
    ) -> None:
        """Class initialization."""

        self.fileobj = fileobj
        self.metadata = metadata
        self.compressed_length = Size.SEEK_SET
        self.data_length = -Size.SEEK_CUR
        self.compression_method = compression_method
        self.compression_level = compression_level
        self.s3_file = s3_file

        if self.fileobj:
            self.csv_start = self.fileobj.tell()

        if self.metadata:
            self.init_metadata(self.metadata)

    @property
    def columns(self) -> list[str]:
        """Get column names."""

        if not self.csv_writer:
            return []

        return self.csv_writer.columns

    @property
    def dtypes(self) -> list[str]:
        """Get column data types."""

        if not self.csv_writer:
            return []

        return self.csv_writer.dtypes

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

        self.csv_writer = CSVWriter(
            self.fileobj,
            self.metadata.csv_metadata,
            self.metadata.delimiter,
            self.metadata.quote_char,
            self.metadata.encoding,
            self.metadata.has_header,
        )

    def from_rows(
        self,
        rows: Iterable[list[Any] | tuple[Any, ...]],
    ) -> int:
        """Convert Python rows to CSVPack format."""

        if not self.metadata:
            raise Error.CSVPackMetadataError("Metadata not defined.")

        return self.from_bytes(self.csv_writer.from_rows(rows))

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

        def validate_state() -> None:
            """Validate expected parameters."""

            if not self.fileobj:
                raise Error.CSVPackValueError("Fileobject not define.")
            if not self.fileobj.writable():
                raise Error.CSVPackModeError("Fileobject don't support write.")
            if not self.metadata:
                raise Error.CSVPackMetadataError("Metadata not defined.")

        def get_compressor() -> CompressorType | None:
            """Get current compressor."""

            if self.compression_method is CompressionMethod.NONE:
                return None

            if isinstance(self.compression_method, CompressionMethod):
                return self.compression_method.compressor(
                    self.compression_level
                )

            raise Error.CSVPackTypeError(
                f"Unsupported compression method {self.compression_method}"
            )

        def write_header() -> None:
            """Write CSVPack header."""

            if not self.csv_writer:
                self.init_metadata(self.metadata)

            self.csv_start = self.fileobj.tell()
            metadata_bytes = bytes(self.metadata)
            metadata_zlib = compress(metadata_bytes)
            metadata_crc = pack(Fmt.U_LONG, crc32(metadata_zlib))
            metadata_length = pack(Fmt.U_LONG, len(metadata_zlib))
            compression_method = pack(
                Fmt.U_CHAR, self.compression_method.value
            )
            s3_marker = (
                Signature.S3_FILE if self.s3_file else bytes(Size.S3_TAIL)
            )

            for data in (
                Signature.HEADER,
                metadata_crc,
                metadata_length,
                metadata_zlib,
                compression_method,
                s3_marker,
            ):
                self.csv_start += self.fileobj.write(data)

        def write_data() -> None:
            """Write CSV data."""

            compressor = get_compressor()

            if compressor:
                compressed = compressor.send_chunks(bytes_data)

                for chunk in compressed:
                    self.fileobj.write(chunk)
                self.data_length = compressor.decompressed_size
            else:
                start_pos = self.fileobj.tell()

                for chunk in bytes_data:
                    self.fileobj.write(chunk)
                self.data_length = self.fileobj.tell() - start_pos

            self.compressed_length = self.fileobj.tell() - self.csv_start

        def write_trailer() -> None:
            """Write compress length and data length."""
            if not self.s3_file:
                self.fileobj.seek(self.csv_start - Size.S3_TAIL)

            self.fileobj.write(
                pack(
                    Fmt.COMPRESS_LENGTH,
                    self.compressed_length,
                    self.data_length,
                )
            )

        validate_state()
        write_header()
        write_data()
        write_trailer()
        self.fileobj.flush()
        return self.tell()

    def tell(self) -> int:
        """Return current position."""

        return self.csv_writer.tell()

    def close(self) -> None:
        """Close file object."""

        self.csv_writer.close()

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
