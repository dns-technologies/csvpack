from light_compressor import CompressionMethod

from .metadata import CSVPackMeta


EMPTY_LINE = "├─────────────────┼─────────────────┤"
END_LINE = "└─────────────────┴─────────────────┘"
HEADER_LINES = [
    "┌─────────────────┬─────────────────┐",
    "│   Column Name   │    Data Type    │",
    "╞═════════════════╪═════════════════╡",
]


def to_col(text: str) -> str:
    """Format string element."""

    return text[:14] + "…" if len(text) > 15 else text


def table_repr(
    columns: list[str],
    dtypes: list[str],
    header: str | None = None,
    tail: list[str] | None = None,
) -> str:
    """Generate table for string representation."""

    table = [
        header,
        *HEADER_LINES,
    ] if header else HEADER_LINES

    for column, dtype in zip(columns, dtypes):
        table.extend([
            f"│ {to_col(column): <15} │ {to_col(dtype): >15} │",
            EMPTY_LINE,
        ])

    table[-1] = END_LINE

    if tail:
        table.extend(tail)

    return "\n".join(table)


def csvlib_repr(
    columns: list[str],
    dtypes: list[str],
    num_columns: int,
    num_rows: int,
    object_type: str,
) -> str:
    """Generate string representation for CSVReader/CSVWriter."""

    return table_repr(
        columns,
        dtypes,
        f"<CSV dump {object_type}>",
        [
            f"Total columns: {num_columns}",
            f"Total rows: {num_rows}",
        ],
    )


def csvpack_repr(
    columns: list[str],
    dtypes: list[str],
    s3_file: bool,
    compressed_length: int,
    data_length: int,
    compression_method: CompressionMethod,
    metadata: CSVPackMeta,
) -> str:
    """Generate string representation for CSVPack."""

    dump_type = "s3file" if s3_file else "dump"
    dump_rate = (compressed_length / data_length) * 100
    return table_repr(
        columns,
        dtypes,
        f"<CSVPack compressed {dump_type}>",
        [
            str(metadata),
            f"Compression Method: {compression_method.name}",
            f"Unpacked Size: {data_length} bytes",
            f"Compressed Size: {compressed_length} bytes",
            f"Compression Rate: {round(dump_rate, 2)} %"
        ],
    )
