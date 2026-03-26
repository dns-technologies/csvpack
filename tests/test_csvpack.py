import pytest
import io
import datetime
import tempfile
from pathlib import Path
import pandas as pd
import polars as pl

from csvpack import (
    CSVPackReader,
    CSVPackWriter,
    CompressionMethod,
    CSVPackMeta,
)


@pytest.fixture
def sample_metadata():
    """Create sample CSVPackMeta object."""

    source = "pandas"
    version = pd.__version__
    timestamp = datetime.datetime(2026, 3, 24, 10, 0, 0)

    source_types = [
        "int64",
        "object",
        "int64",
        "bool",
        "float64",
        "datetime64[ns]",
        "datetime64[ns]",
        "object",
    ]

    csv_metadata = [
        {"id": "int"},
        {"name": "str"},
        {"age": "int"},
        {"active": "bool"},
        {"salary": "float"},
        {"created_date": "date"},
        {"created_datetime": "datetime"},
        {"tags": "list[str]"},
    ]

    return CSVPackMeta(
        source=source,
        version=version,
        timestamp=timestamp,
        source_types=source_types,
        csv_metadata=csv_metadata,
        delimiter=",",
        quote_char='"',
        encoding="utf-8",
        has_header=True,
    )

@pytest.fixture
def sample_metadata_bytes(sample_metadata: CSVPackMeta):
    """Create sample metadata as bytes."""

    return sample_metadata.to_bytes()

@pytest.fixture
def sample_metadata_full():
    """Create sample CSVPackMeta object for full 17-column dataset."""

    source = "postgres"
    version = "14"
    timestamp = datetime.datetime(2026, 3, 24, 10, 0, 0)

    source_types = [
        "date",
        "date",
        "varchar(200)",
        "varchar(200)",
        "varchar(200)",
        "uuid",
        "uuid",
        "text",
        "text",
        "int4",
        "int4",
        "int4",
        "int4",
        "int4",
        "int4",
        "int4",
        "int4",
    ]

    csv_metadata = [
        {"start_month": "date"},
        {"start_day": "date"},
        {"division_name": "str"},
        {"rdc_name": "str"},
        {"branch_name": "str"},
        {"branch_guid": "uuid"},
        {"category_guid": "uuid"},
        {"category_name": "str"},
        {"bonus_type": "str"},
        {"category_rn": "int"},
        {"tso_metric1_rn": "int"},
        {"tso_metric2_rn": "int"},
        {"employee_total_rn": "int"},
        {"category_pcs": "int"},
        {"employee_tso_metric1": "int"},
        {"employee_tso_metric2": "int"},
        {"employee_tso_metric3": "int"},
    ]

    return CSVPackMeta(
        source=source,
        version=version,
        timestamp=timestamp,
        source_types=source_types,
        csv_metadata=csv_metadata,
        delimiter=",",
        quote_char='"',
        encoding="utf-8",
        has_header=True,
    )

@pytest.fixture
def sample_rows():
    """Create sample Python rows."""

    return [
        (
            1,
            "Alice",
            25,
            True,
            50000.5,
            datetime.date(2024, 1, 1),
            datetime.datetime(2024, 1, 1, 10, 0, 0),
            ["python", "data"],
        ),
        (
            2,
            "Bob",
            30,
            False,
            60000.0,
            datetime.date(2024, 1, 2),
            datetime.datetime(2024, 1, 2, 10, 0, 0),
            ["rust", "csv"],
        ),
        (
            3,
            "Charlie",
            35,
            True,
            75000.75,
            datetime.date(2024, 1, 3),
            datetime.datetime(2024, 1, 3, 10, 0, 0),
            ["pandas"],
        ),
    ]

@pytest.fixture
def sample_dataframe():
    """Create sample pandas DataFrame."""

    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "age": [25, 30, 35, 28, 32],
            "active": [True, False, True, False, True],
            "salary": [50000.5, 60000.0, 75000.75, 48000.25, 82000.0],
            "created_date": pd.date_range("2024-01-01", periods=5),
            "created_datetime": pd.date_range(
                "2024-01-01 10:00:00", periods=5
            ),
            "tags": [
                ["python", "data"],
                ["rust", "csv"],
                ["pandas"],
                [],
                ["numpy", "polars"],
            ],
        }
    )

class TestCSVPack:
    """Tests for CSVPackReader and CSVPackWriter."""

    def test_write_and_read_pandas(
        self,
        sample_dataframe: pd.DataFrame | pl.DataFrame,
    ):
        """Test writing and reading pandas DataFrame."""

        buffer = io.BytesIO()
        writer = CSVPackWriter(fileobj=buffer)
        writer.from_pandas(sample_dataframe)
        buffer.seek(0)
        reader = CSVPackReader(buffer)
        assert reader.columns == list(sample_dataframe.columns)  # noqa: S101
        assert len(reader.dtypes) == len(sample_dataframe.columns)  # noqa: S101
        df_result = reader.to_pandas()
        assert len(df_result) == len(sample_dataframe)  # noqa: S101
        assert list(df_result.columns) == list(sample_dataframe.columns)  # noqa: S101
        reader.close()

    def test_write_and_read_polars(
        self,
        sample_dataframe: pd.DataFrame | pl.DataFrame,
    ):
        """Test writing and reading polars DataFrame."""

        buffer = io.BytesIO()
        pl_df: pl.DataFrame = pl.from_pandas(sample_dataframe)
        writer = CSVPackWriter(fileobj=buffer)
        writer.from_polars(pl_df)
        buffer.seek(0)
        reader = CSVPackReader(buffer)
        df_result = reader.to_polars()
        assert len(df_result) == len(pl_df)  # noqa: S101
        assert df_result.columns == pl_df.columns  # noqa: S101
        reader.close()

    def test_write_and_read_polars_lazy(self, sample_dataframe):
        """Test writing and reading lazy polars DataFrame."""

        buffer = io.BytesIO()
        pl_df: pl.DataFrame = pl.from_pandas(sample_dataframe)
        writer = CSVPackWriter(fileobj=buffer)
        writer.from_polars(pl_df.lazy())
        buffer.seek(0)
        reader = CSVPackReader(buffer)
        df_result = reader.to_polars(is_lazy=True)
        assert isinstance(df_result, pl.LazyFrame)  # noqa: S101
        collected = df_result.collect()
        assert len(collected) == len(sample_dataframe)  # noqa: S101
        reader.close()

    def test_write_and_read_rows(self, sample_rows, sample_metadata):
        """Test writing and reading Python rows."""

        buffer = io.BytesIO()
        writer = CSVPackWriter(fileobj=buffer)
        writer.init_metadata(sample_metadata)
        writer.from_rows(sample_rows)
        buffer.seek(0)
        reader = CSVPackReader(buffer)
        rows = list(reader.to_rows())
        assert len(rows) == len(sample_rows)  # noqa: S101

        for original, read in zip(sample_rows, rows):
            assert original[0] == read[0]  # noqa: S101
            assert original[1] == read[1]  # noqa: S101
            assert original[2] == read[2]  # noqa: S101
            assert original[3] == read[3]  # noqa: S101
            assert original[4] == read[4]  # noqa: S101
            assert original[5] == read[5]  # noqa: S101
            assert original[6] == read[6]  # noqa: S101
            assert original[7] == read[7]  # noqa: S101

        reader.close()

    def test_to_bytes(self, sample_dataframe):
        """Test reading as bytes chunks."""

        buffer = io.BytesIO()
        writer = CSVPackWriter(fileobj=buffer)
        writer.from_pandas(sample_dataframe)
        buffer.seek(0)
        reader = CSVPackReader(buffer)
        chunks = list(reader.to_bytes())
        assert len(chunks) > 0  # noqa: S101
        full_data = b"".join(chunks)
        assert (  # noqa: S101
            b"1,Alice,25,1,50000.5,2024-01-01 00:00:00,2024-01-01 10:00:00,"
            b"\"['python','data']\"\n"
            in full_data
        )
        reader.close()

    def test_all_compression_methods(self, sample_dataframe):
        """Test all compression methods."""
        methods = [
            CompressionMethod.NONE,
            CompressionMethod.GZIP,
            CompressionMethod.LZ4,
            CompressionMethod.SNAPPY,
            CompressionMethod.ZSTD,
        ]

        for method in methods:
            buffer = io.BytesIO()
            writer = CSVPackWriter(
                fileobj=buffer, compression_method=method, compression_level=3
            )
            writer.from_pandas(sample_dataframe)
            assert buffer.tell() > 0  # noqa: S101
            buffer.seek(0)
            reader = CSVPackReader(buffer)
            df_result = reader.to_pandas()
            assert len(df_result) == len(sample_dataframe)  # noqa: S101
            reader.close()

    def test_s3_file_mode(self, sample_dataframe):
        """Test S3 file mode."""

        buffer = io.BytesIO()
        writer = CSVPackWriter(
            fileobj=buffer,
            compression_method=CompressionMethod.ZSTD,
            s3_file=True,
        )
        writer.from_pandas(sample_dataframe)
        assert writer.compressed_length > 0  # noqa: S101
        assert writer.data_length > 0  # noqa: S101
        buffer.seek(0)
        reader = CSVPackReader(buffer)
        assert reader.s3_file is True  # noqa: S101
        assert reader.compressed_length > 0  # noqa: S101
        assert reader.data_length > 0  # noqa: S101
        df_result = reader.to_pandas()
        assert len(df_result) == len(sample_dataframe)  # noqa: S101
        reader.close()

    def test_metadata_properties(self, sample_dataframe):
        """Test metadata properties."""

        buffer = io.BytesIO()
        writer = CSVPackWriter(fileobj=buffer)
        writer.from_pandas(sample_dataframe)
        buffer.seek(0)
        reader = CSVPackReader(buffer)
        assert reader.metadata.source == "Pandas"  # noqa: S101
        assert reader.metadata.source_types is not None  # noqa: S101
        assert len(reader.metadata.csv_metadata) == len(  # noqa: S101
            sample_dataframe.columns
        )
        assert reader.metadata.delimiter == ","  # noqa: S101
        assert reader.metadata.quote_char == '"'  # noqa: S101
        assert reader.metadata.encoding == "utf-8"  # noqa: S101
        assert reader.metadata.has_header is False  # noqa: S101
        reader.close()

    def test_empty_dataframe(self):
        """Test with empty DataFrame."""

        df = pd.DataFrame(columns=["id", "name", "age"])
        buffer = io.BytesIO()
        writer = CSVPackWriter(fileobj=buffer)
        writer.from_pandas(df)
        buffer.seek(0)
        reader = CSVPackReader(buffer)
        rows = list(reader.to_rows())
        assert len(rows) == 0  # noqa: S101
        df_result = reader.to_pandas()
        assert len(df_result) == 0  # noqa: S101
        assert list(df_result.columns) == ["id", "name", "age"]  # noqa: S101
        reader.close()

    def test_with_nested_lists(self):
        """Test with nested lists data."""

        df = pd.DataFrame(
            {
                "id": [1, 2],
                "matrix": [[[1, 2], [3, 4]], [[5, 6], [7, 8]]],
                "list_of_dicts": [[{"a": 1}, {"b": 2}], [{"c": 3}, {"d": 4}]],
            }
        )
        buffer = io.BytesIO()
        writer = CSVPackWriter(fileobj=buffer)
        writer.from_pandas(df)
        buffer.seek(0)
        reader = CSVPackReader(buffer)
        df_result = reader.to_pandas()
        assert len(df_result) == len(df)  # noqa: S101
        reader.close()

    def test_with_special_characters(self):
        """Test with special characters in data."""

        df = pd.DataFrame(
            {
                "text": [
                    'Hello, "World"!',
                    "It's a test",
                    "Field with, comma",
                    'Field with "quotes" and , comma',
                ]
            }
        )
        buffer = io.BytesIO()
        writer = CSVPackWriter(fileobj=buffer)
        writer.from_pandas(df)
        buffer.seek(0)
        reader = CSVPackReader(buffer)
        df_result = reader.to_pandas()
        assert df_result["text"].tolist() == df["text"].tolist()  # noqa: S101
        reader.close()

    def test_tell_method(self, sample_dataframe):
        """Test tell method."""

        buffer = io.BytesIO()
        writer = CSVPackWriter(fileobj=buffer)
        writer.from_pandas(sample_dataframe)
        pos = writer.tell()
        assert pos >= 0  # noqa: S101
        buffer.seek(0)
        reader = CSVPackReader(buffer)
        pos = reader.tell()
        assert pos == 0  # noqa: S101
        list(reader.to_rows())
        assert reader.tell() > 0  # noqa: S101
        reader.close()

    def test_close_method(self, sample_dataframe):
        """Test close method."""

        buffer = io.BytesIO()
        writer = CSVPackWriter(fileobj=buffer)
        writer.from_pandas(sample_dataframe)
        writer.close()
        assert buffer.closed is True  # noqa: S101
        buffer = io.BytesIO()
        writer = CSVPackWriter(fileobj=buffer)
        writer.from_pandas(sample_dataframe)
        buffer.seek(0)
        reader = CSVPackReader(buffer)
        assert buffer.closed is False  # noqa: S101
        reader.close()
        assert buffer.closed is True  # noqa: S101

    def test_multiple_writes(self, sample_dataframe):
        """Test writing multiple DataFrames."""

        buffer = io.BytesIO()
        writer = CSVPackWriter(fileobj=buffer)
        writer.from_pandas(sample_dataframe)
        buffer.seek(0)
        reader = CSVPackReader(buffer)
        df1 = reader.to_pandas()
        reader.close()
        buffer2 = io.BytesIO()
        writer2 = CSVPackWriter(fileobj=buffer2)
        writer2.from_pandas(sample_dataframe)
        buffer2.seek(0)
        reader2 = CSVPackReader(buffer2)
        df2 = reader2.to_pandas()
        reader2.close()
        pd.testing.assert_frame_equal(df1, df2)

    def test_file_operations(self, sample_dataframe):
        """Test with real file."""

        with tempfile.NamedTemporaryFile(
            mode="wb+", suffix=".csvpack", delete=False
        ) as tmp:
            writer = CSVPackWriter(fileobj=tmp)
            writer.from_pandas(sample_dataframe)
            writer.close()

            with open(tmp.name, "rb") as f:
                reader = CSVPackReader(f)
                df_result = reader.to_pandas()
                reader.close()

            assert len(df_result) == len(sample_dataframe)  # noqa: S101

        Path(tmp.name).unlink()


class TestCSVPackEdgeCases:
    """Edge cases tests for CSVPack."""

    def test_single_row(self):
        """Test with single row DataFrame."""

        df = pd.DataFrame({"col1": [1], "col2": ["test"]})
        buffer = io.BytesIO()
        writer = CSVPackWriter(fileobj=buffer)
        writer.from_pandas(df)
        buffer.seek(0)
        reader = CSVPackReader(buffer)
        df_result = reader.to_pandas()
        assert len(df_result) == 1  # noqa: S101
        assert df_result["col1"].iloc[0] == 1  # noqa: S101
        assert df_result["col2"].iloc[0] == "test"  # noqa: S101
        reader.close()

    def test_single_column(self):
        """Test with single column DataFrame."""

        df = pd.DataFrame({"single_column": [1, 2, 3, 4, 5]})
        buffer = io.BytesIO()
        writer = CSVPackWriter(fileobj=buffer)
        writer.from_pandas(df)
        buffer.seek(0)
        reader = CSVPackReader(buffer)
        df_result = reader.to_pandas()
        assert list(df_result.columns) == ["single_column"]  # noqa: S101
        assert len(df_result) == 5  # noqa: S101
        reader.close()

    def test_with_none_values(self):
        """Test with None/NaN values."""

        df = pd.DataFrame(
            {
                "int_col": [1, None, 3, None, 5],
                "str_col": ["a", None, "c", None, "e"],
                "float_col": [1.5, None, 3.5, None, 5.5],
            }
        )
        buffer = io.BytesIO()
        writer = CSVPackWriter(fileobj=buffer)
        writer.from_pandas(df)
        buffer.seek(0)
        reader = CSVPackReader(buffer)
        df_result = reader.to_pandas()
        assert df_result["int_col"].isna().iloc[1]  # noqa: S101
        assert df_result["str_col"].isna().iloc[1]  # noqa: S101
        assert df_result["float_col"].isna().iloc[1]  # noqa: S101
        reader.close()

    def test_large_dataframe(self):
        """Test with larger DataFrame."""

        df = pd.DataFrame(
            {
                "id": range(10000),
                "value": [i * 1.5 for i in range(10000)],
                "text": [f"text_{i}" for i in range(10000)],
            }
        )
        buffer = io.BytesIO()
        writer = CSVPackWriter(
            fileobj=buffer, compression_method=CompressionMethod.LZ4
        )
        writer.from_pandas(df)
        buffer.seek(0)
        reader = CSVPackReader(buffer)
        df_result = reader.to_pandas()
        assert len(df_result) == 10000  # noqa: S101
        assert df_result["id"].iloc[0] == 0  # noqa: S101
        assert df_result["id"].iloc[-1] == 9999  # noqa: S101
        reader.close()

    def test_unseekable_stream(self, sample_dataframe):
        """Test with unseekable stream (simulate socket)."""

        class MockSocket:
            def __init__(self, data):
                self.data = data
                self.pos = 0

            def read(self, size):
                if self.pos >= len(self.data):
                    return b""
                result = self.data[self.pos : self.pos + size]
                self.pos += len(result)
                return result

            def tell(self):
                return self.pos

            def seekable(self):
                return False

        buffer = io.BytesIO()
        writer = CSVPackWriter(fileobj=buffer)
        writer.from_pandas(sample_dataframe)
        buffer.seek(0)
        data = buffer.getvalue()
        stream = MockSocket(data)
        reader = CSVPackReader(stream)
        assert reader.s3_file is False  # noqa: S101
        rows = list(reader.to_rows())
        assert len(rows) == len(sample_dataframe)  # noqa: S101
        reader.close()


class TestCSVPackRepr:
    """Tests for CSVPack representation."""

    def test_reader_repr(self, sample_dataframe):
        """Test CSVPackReader __repr__."""

        buffer = io.BytesIO()
        writer = CSVPackWriter(fileobj=buffer)
        writer.from_pandas(sample_dataframe)
        buffer.seek(0)
        reader = CSVPackReader(buffer)
        repr_str = repr(reader)
        assert "<CSVPack compressed dump>" in repr_str  # noqa: S101
        assert "Total columns: 8" in repr_str  # noqa: S101
        assert "Compression method: ZSTD" in repr_str  # noqa: S101
        reader.close()

    def test_writer_repr(self, sample_dataframe):
        """Test CSVPackWriter __repr__ after write."""

        buffer = io.BytesIO()
        writer = CSVPackWriter(fileobj=buffer)
        writer.from_pandas(sample_dataframe)
        repr_str = repr(writer)
        assert "<CSVPack compressed dump>" in repr_str  # noqa: S101
        assert "Total columns: 8" in repr_str  # noqa: S101
        writer.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
