# CSVPack format

Storage format for CSV dump packed into GZIP, LZ4, SNAPPY, ZSTD or uncompressed with meta data information packed into zlib.

## CSVPack structure

- header `b"CSVPACK\n"` 8 bytes
- unsigned long integer zlib.crc32 for packed metadata 4 bytes
- unsigned long integer zlib packed metadata length 4 bytes
- zlib packed metadata (JSON)
- unsigned char compression method 1 byte
- unsigned long long integer packed CSV data length 8 bytes (or `0x5333005374726561` for S3 file mode)
- unsigned long long integer unpacked CSV data length 8 bytes (or `0x6d004f626a656374` for S3 file mode)
- compressed CSV data
- (S3 mode only) 16 bytes tail with compressed and uncompressed lengths at the end of file

## Installation

### From pip

```bash
pip install csvpack -U --index-url https://dns-technologies.github.io/dbhose-dev-pip/simple/
```
### From local directory

```bash
pip install . --extra-index-url https://dns-technologies.github.io/dbhose-dev-pip/simple/
```

### From git

```bash
pip install git+https://github.com/dns-technologies/csvpack --extra-index-url https://dns-technologies.github.io/dbhose-dev-pip/simple/
```

## Metadata format

Metadata contains column names and their data types in a JSON format.

### Decompressed metadata structure

```json
[
    "source",           // Data source name (e.g., "pandas", "polars", "postgres")
    "version",          // Source library version
    "timestamp",        // ISO format timestamp
    ["type1", "type2"], // Original data types from source
    [
        {"column1": "csv_type1"},
        {"column2": "csv_type2"}
    ],                  // CSV metadata with column types
    "delimiter",        // CSV delimiter character
    "quote_char",       // CSV quote character
    "encoding",         // Character encoding
    "has_header",       // Boolean indicating if CSV has header
]
```

### Compression methods

* `NONE` (value = `0x02`) — CSV dump without compression
* `GZIP` (value = `0x99`) — CSV dump with GZIP compression
* `LZ4` (value = `0x82`) — CSV dump with LZ4 compression
* `SNAPPY` (value = `0x9f`) — CSV dump with Snappy compression
* `ZSTD` (value = `0x90`) — CSV dump with Zstandard compression (default)

### Get ENUM for set compression method

```python
from csvpack import CompressionMethod

compression_method = CompressionMethod.NONE   # no compression
compression_method = CompressionMethod.GZIP   # gzip compression
compression_method = CompressionMethod.LZ4    # lz4 compression
compression_method = CompressionMethod.SNAPPY # snappy compression
compression_method = CompressionMethod.ZSTD   # zstd compression (default)
```

## Class CSVPackReader

### Initialization parameters:
- `fileobj` — BufferedReader object (file, BytesIO, etc.)

### Methods and attributes:
- `metadata` — CSVPackMeta object with metadata
- `columns` — List of column names
- `dtypes` — List of CSV data types for all columns
- `compressed_length` — integer packed CSV data length
- `data_length` — integer unpacked CSV data length
- `compression_method` — CompressionMethod object
- `compression_stream` — BufferedReader object for decompressed data
- `s3_file` — bool to detect file mode (dump or s3file)
- `csv_start` — integer offset for start of CSV data
- `csv_reader` — CSVReader object
- `to_rows()` — Method for reading uncompressed CSV data as generator of Python objects
- `to_pandas()` — Method for reading uncompressed CSV data as pandas.DataFrame
- `to_polars(is_lazy=False)` — Method for reading uncompressed CSV data as polars.DataFrame/LazyFrame
- `to_bytes()` — Method for reading uncompressed CSV data as generator of bytes
- `tell()` — Return current position
- `close()` — Close file object

---

## Class CSVPackWriter

### Initialization parameters:
- `fileobj` — BufferedWriter object (file, BytesIO, etc.) or None
- `metadata` — metadata in bytes or CSVPackMeta object (default is None)
- `compression_method` — CompressionMethod object (default is CompressionMethod.ZSTD)
- `compression_level` — int value for compression level (default is 3)
- `s3_file` — bool for selecting write mode between dump and s3file (default is False)

### Methods and attributes:
- `columns` — List of column names
- `dtypes` — List of CSV data types for all columns
- `compressed_length` — integer packed CSV data length (set to 0 on initialization)
- `data_length` — integer unpacked CSV data length (set to -1 on initialization)
- `csv_start` — integer offset for start of CSV data (set to current offset on initialization)
- `csv_writer` — CSVWriter object
- `init_metadata(metadata)` — Initialize metadata and CSV writer
- `from_rows(rows)` — Write CSVPack file from Python objects (iterable of rows)
- `from_pandas(data_frame)` — Write CSVPack file from pandas.DataFrame
- `from_polars(data_frame)` — Write CSVPack file from polars.DataFrame/LazyFrame
- `from_bytes(bytes_data)` — Write CSVPack file from bytes (iterable of bytes chunks)
- `tell()` — Return current position
- `close()` — Close file object

---

## Errors

- `CSVPackError` — Base CSVPack error
- `CSVPackHeaderError` — Error header signature
- `CSVPackMetadataError` — Error metadata
- `CSVPackMetadataCrcError` — Error metadata crc32
- `CSVPackModeError` — Error file object mode
- `CSVPackTypeError` — Error type mismatch
- `CSVPackValueError` — Error value mismatch

## Examples

### Write and read CSVPack file

```python
import io
from csvpack import CSVPackWriter, CSVPackReader, CompressionMethod
import pandas as pd

# Create a pandas DataFrame
df = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35]
})

# Write to CSVPack format
buffer = io.BytesIO()
writer = CSVPackWriter(
    fileobj=buffer,
    compression_method=CompressionMethod.ZSTD,
    compression_level=3
)
writer.from_pandas(df)

# Read from CSVPack format
buffer.seek(0)
reader = CSVPackReader(buffer)
print(reader.columns)          # ['id', 'name', 'age']
print(reader.dtypes)           # ['int', 'str', 'int']

# Get as pandas DataFrame
df_result = reader.to_pandas()
print(df_result)

# Get as polars DataFrame
pl_result = reader.to_polars()
print(pl_result)

# Get as generator of rows
for row in reader.to_rows():
    print(row)

reader.close()
```

### Write from Python rows

```python
import io
from csvpack import (
    CompressionMethod,
    CSVPackMeta,
    CSVPackWriter,
)

rows = [
    (1, 'Alice', 25),
    (2, 'Bob', 30),
    (3, 'Charlie', 35)
]

metadata = CSVPackMeta.from_params(
    source="Python",
    version="3.10",
    columns=["id", "name", "age"],
    dtypes=["int", "str", "int"],
)

buffer = io.BytesIO()
writer = CSVPackWriter(
    fileobj=buffer,
    compression_method=CompressionMethod.ZSTD,
    compression_level=3
)
writer.init_metadata(metadata)
writer.from_rows(rows)
writer.close()
```

### S3 file mode (compatible with streaming upload)

```python
import io
from csvpack import CSVPackWriter, CSVPackReader, CompressionMethod

# Write in S3 mode (metadata at beginning, lengths at end)
buffer = io.BytesIO()
writer = CSVPackWriter(
    fileobj=buffer,
    compression_method=CompressionMethod.ZSTD,
    s3_file=True
)
writer.from_pandas(df)

# Read S3 mode file
buffer.seek(0)
reader = CSVPackReader(buffer)
df_result = reader.to_pandas()
reader.close()
```

### All compression methods comparison

```python
import io
import time
from csvpack import CSVPackWriter, CSVPackReader, CompressionMethod
import pandas as pd

# Create large DataFrame
df = pd.DataFrame({
    'id': range(100000),
    'text': ['some text here'] * 100000,
    'numbers': [i * 1.5 for i in range(100000)]
})

methods = [
    CompressionMethod.NONE,
    CompressionMethod.GZIP,
    CompressionMethod.LZ4,
    CompressionMethod.SNAPPY,
    CompressionMethod.ZSTD
]

for method in methods:
    buffer = io.BytesIO()
    
    # Write
    start = time.time()
    writer = CSVPackWriter(
        fileobj=buffer,
        compression_method=method,
        compression_level=3
    )
    writer.from_pandas(df)
    write_time = time.time() - start
    
    # Read
    buffer.seek(0)
    start = time.time()
    reader = CSVPackReader(buffer)
    df_result = reader.to_pandas()
    read_time = time.time() - start
    reader.close()
    
    print(f"{method.name}:")
    print(f"  Size: {buffer.tell() / 1024:.2f} KB")
    print(f"  Write: {write_time:.2f}s, Read: {read_time:.2f}s")
    print()
```

### Data Type Mapping

CSVPack automatically maps source data types to CSV-compatible types:

| Source Type | CSV Type | Description |
|-------------|----------|-------------|
| `int`, `Int8`, `Int16`, `Int32`, `Int64`, `UInt8`, `UInt16`, `UInt32`, `UInt64` | `int` | Integer numbers |
| `float`, `Float32`, `Float64`, `Decimal` | `float` | Floating point numbers |
| `bool`, `Boolean` | `bool` | Boolean values |
| `str`, `String`, `Utf8` | `str` | String values |
| `date`, `Date` | `date` | Date values |
| `datetime`, `Datetime`, `Timestamp` | `datetime` | DateTime values |
| `time`, `Time` | `time` | Time values |
| `uuid`, `UUID` | `uuid` | UUID values |
| `list`, `Array` | `list[T]` | Lists with element type T |
| `json`, `JSON`, `Struct`, `Map` | `json` | JSON objects |
| `bytes`, `Binary` | `bytes` | Binary data |

For nested structures like list[int] or list[datetime], the type is preserved in the CSV format.

## Development

### Running tests

```bash
pytest tests/ -v
```

### Building from source

```bash
pip install . --no-cache --extra-index-url https://dns-technologies.github.io/dbhose-dev-pip/simple/
```
