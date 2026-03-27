from datetime import (
    date,
    datetime,
)
from json import (
    JSONEncoder,
    dumps,
    loads,
)
from typing import (
    Any,
    NamedTuple,
)

from pandas import DataFrame as PdFrame
from polars import DataFrame as PlFrame

from .dtype import PD_TYPE
from .finder import from_dtypes


class JsonEncTime(JSONEncoder):
    """Json encoder with datetime support."""

    def default(self, obj):
        """Convert datetime and date to ISO."""

        if isinstance(obj, date):
            return obj.isoformat()

        return super().default(obj)


class CSVPackMeta(NamedTuple):
    """Metadata for CSVPack."""

    source: str
    version: str
    timestamp: datetime
    source_types: list[str]
    csv_metadata: list[dict[str, str]]
    delimiter: str = ","
    quote_char: str = '"'
    encoding: str = "utf-8"
    has_header: bool = False

    @property
    def pandas_astype(self) -> dict[str, str]:
        """Make pandas dtypes from columns."""

        return {
            column: PD_TYPE.get(ptype)
            for columns in self.csv_metadata
            for column, ptype in columns.items()
        }

    @classmethod
    def from_params(
        cls,
        source: str,
        version: str,
        columns: list[str],
        dtypes: list[Any],
        timestamp: datetime | None = None,
        delimiter: str = ",",
        quote_char: str = '"',
        encoding: str = "utf-8",
        has_header: bool = False,
    ) -> "CSVPackMeta":
        """Make object from params."""

        if not timestamp:
            timestamp = datetime.now()

        source_types = [str(dtype) for dtype in dtypes]
        ptypes = from_dtypes(source, source_types)
        csv_metadata = [
            {column: ptype}
            for column, ptype in zip(columns, ptypes)
        ]
        return cls(
            source,
            version,
            timestamp,
            source_types,
            csv_metadata,
            delimiter,
            quote_char,
            encoding,
            has_header,
        )

    @classmethod
    def from_bytes(cls, metadata: bytes) -> "CSVPackMeta":
        """Make object from bytes."""

        csvpack_meta = loads(metadata)
        csvpack_meta[2] = datetime.fromisoformat(csvpack_meta[2])
        return cls(*csvpack_meta)

    def to_bytes(self) -> bytes:
        """Convert object to bytes."""

        return dumps(
            self,
            ensure_ascii=False,
            cls=JsonEncTime,
        ).encode("utf-8")

    def __bytes__(self) -> bytes:
        """Bytes representation of CSVPackMeta."""

        return self.to_bytes()

    def __repr__(self) -> str:
        """String representation of CSVPackMeta."""

        return f"""\
Source: {self.source.capitalize()}
Version: {self.version}
Original Types: [{", ".join(self.source_types)}]
Total Columns: {len(self.source_types)}
Upload Timestamp: {self.timestamp.strftime("%Y-%m-%d %H:%M:%S")}\
"""


def metadata_from_frame(
    data_frame: PdFrame | PlFrame,
) -> CSVPackMeta:
    """Generate CSVPackMeta from pandas.DataFrame/polars.DataFrame."""

    source = data_frame.__class__.__module__.split(".")[0]
    version = getattr(__import__(source), "__version__", "unknown")

    return CSVPackMeta.from_params(
        source.capitalize(),
        version,
        data_frame.columns,
        data_frame.dtypes,
    )
