"""Library for read and write storage format for CSV dump
packed into GZIP, LZ4, SNAPPY, ZSTD or uncompressed
with meta data information packed into zlib."""

from light_compressor import (
    CompressionLevel,
    CompressionMethod,
)

from .common.errors import (
    CSVPackError,
    CSVPackHeaderError,
    CSVPackMetadataCrcError,
    CSVPackMetadataError,
    CSVPackModeError,
    CSVPackTypeError,
    CSVPackValueError,
)
from .common.metadata import CSVPackMeta
from .csvlib import (
    CSVReader,
    CSVWriter,
    RustCsvReader,
    RustCsvWriter,
)
from .reader import CSVPackReader
from .writer import CSVPackWriter


__all__ = (
    "CompressionLevel",
    "CompressionMethod",
    "CSVPackError",
    "CSVPackHeaderError",
    "CSVPackMeta",
    "CSVPackMetadataCrcError",
    "CSVPackMetadataError",
    "CSVPackModeError",
    "CSVPackReader",
    "CSVPackTypeError",
    "CSVPackValueError",
    "CSVPackWriter",
    "CSVReader",
    "CSVWriter",
    "RustCsvReader",
    "RustCsvWriter",
)
__author__ = "0xMihalich"
__version__ = "0.1.0.dev2"
