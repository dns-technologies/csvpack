import datetime
import io
import tempfile
import uuid

from pathlib import Path

import pytest

from csvpack import (
    CSVReader,
    CSVWriter,
)


expected_columns = [
    "start_month",
    "start_day",
    "division_name",
    "rdc_name",
    "branch_name",
    "branch_guid",
    "category_guid",
    "category_name",
    "bonus_type",
    "category_rn",
    "tso_metric1_rn",
    "tso_metric2_rn",
    "employee_total_rn",
    "category_pcs",
    "employee_tso_metric1",
    "employee_tso_metric2",
    "employee_tso_metric3",
]
expected_dtypes = [
    "date",
    "date",
    "str",
    "str",
    "str",
    "uuid",
    "uuid",
    "str",
    "str",
    "int",
    "int",
    "int",
    "int",
    "int",
    "int",
    "int",
    "int",
]


@pytest.fixture
def sample_metadata():
    """Sample metadata for tests."""

    return [
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


@pytest.fixture
def sample_row():
    """Sample row for tests."""

    return (
        datetime.date(2026, 3, 1),
        datetime.date(2026, 3, 24),
        "Дивизион 4",
        "РРС Центр",
        "ООО Рога и копыта",
        uuid.UUID("36929e13-2a94-4810-ba49-3e41466c899f"),
        uuid.UUID("f6924c17-8c62-41e3-a10f-00155d031652"),
        "Новая Лада",
        "Можно, а зачем?",
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
    )


@pytest.fixture
def metadata_with_lists():
    """Metadata with list types."""

    return [
        {"id": "int"},
        {"names": "list[str]"},
        {"values": "list[int]"},
        {"timestamps": "list[datetime]"},
    ]


@pytest.fixture
def row_with_lists():
    """Row with list data."""

    return (
        1,
        ["John", "Jane", "Bob"],
        [10, 20, 30],
        [
            datetime.datetime(2024, 10, 1, 10, 0, 0),
            datetime.datetime(2024, 10, 2, 14, 30, 0),
        ],
    )


@pytest.fixture
def reader(sample_metadata, sample_row):
    """Create reader with sample data."""

    buffer = io.BytesIO()
    writer = CSVWriter(metadata=sample_metadata, fileobj=buffer)
    writer.write([sample_row])
    buffer.seek(0)
    return CSVReader(
        fileobj=buffer,
        metadata=sample_metadata,
        delimiter=",",
        has_header=True,
    )


class TestCSV:
    """Тесты для CSV reader/writer."""

    def test_writer_basic(self, sample_metadata, sample_row):
        """Test basic write operation."""

        output = io.BytesIO()
        writer = CSVWriter(metadata=sample_metadata, fileobj=output)
        writer.write([sample_row])
        result = output.getvalue().decode("utf-8")
        assert "start_month" in result  # noqa: S101
        assert "branch_guid" in result  # noqa: S101
        assert "2026-03-01" in result  # noqa: S101
        assert "36929e13-2a94-4810-ba49-3e41466c899f" in result  # noqa: S101
        assert "Дивизион 4" in result  # noqa: S101
        assert "Новая Лада" in result  # noqa: S101

    def test_writer_multiple_rows(self, sample_metadata, sample_row):
        """Test writing multiple rows."""

        output = io.BytesIO()
        writer = CSVWriter(metadata=sample_metadata, fileobj=output)
        writer.write([sample_row, sample_row])
        result = output.getvalue().decode("utf-8")
        lines = result.strip().split("\n")
        assert len(lines) == 3  # noqa: S101

    def test_writer_from_rows(self, sample_metadata, sample_row):
        """Test from_rows method."""

        output = io.BytesIO()
        writer = CSVWriter(metadata=sample_metadata, fileobj=output)
        rows = [sample_row, sample_row]
        writer.write(rows)
        result = output.getvalue().decode("utf-8")
        lines = result.strip().split("\n")
        assert len(lines) == 3  # noqa: S101

    def test_writer_chunking(self, sample_metadata, sample_row):
        """Test chunking with small buffer."""

        output = io.BytesIO()
        writer = CSVWriter(
            metadata=sample_metadata,
            fileobj=output,
        )
        rows = [sample_row for _ in range(100)]
        chunks = list(writer.from_rows(rows))
        assert len(chunks) >= 1  # noqa: S101
        full_data = b"".join(chunks)
        lines = full_data.split(b'\n')
        assert b"start_month" in lines[0]  # noqa: S101
        assert len(lines) >= 101  # noqa: S101

    def test_read_write_roundtrip(self, sample_metadata, sample_row):
        """Test write and read roundtrip."""

        buffer = io.BytesIO()
        writer = CSVWriter(metadata=sample_metadata, fileobj=buffer)
        writer.write([sample_row])
        buffer.seek(0)
        reader = CSVReader(
            fileobj=buffer,
            metadata=sample_metadata,
            delimiter=",",
            has_header=True,
        )
        rows = list(reader)
        assert len(rows) == 1  # noqa: S101
        read_row = rows[0]
        assert read_row[0] == sample_row[0]  # noqa: S101
        assert read_row[1] == sample_row[1]  # noqa: S101
        assert read_row[2] == sample_row[2]  # noqa: S101
        assert read_row[5] == sample_row[5]  # noqa: S101
        assert read_row[7] == sample_row[7]  # noqa: S101
        assert read_row[9] == sample_row[9]  # noqa: S101

    def test_file_operations(self, sample_metadata, sample_row):
        """Test with real file."""

        with tempfile.NamedTemporaryFile(
            mode="wb+", suffix=".csv", delete=False
        ) as tmp:
            writer = CSVWriter(metadata=sample_metadata, fileobj=tmp)
            writer.write([sample_row])
            writer.close()

            with open(tmp.name, "rb") as f:
                reader = CSVReader(
                    fileobj=f,
                    metadata=sample_metadata,
                    delimiter=",",
                    has_header=True,
                )
                rows = list(reader)

            assert len(rows) == 1  # noqa: S101
            assert rows[0][2] == "Дивизион 4"  # noqa: S101

        Path(tmp.name).unlink()


class TestCSVWithLists:
    """Тесты для работы со списками."""

    def test_write_list(self, metadata_with_lists, row_with_lists):
        """Test writing rows with lists."""

        output = io.BytesIO()
        writer = CSVWriter(metadata=metadata_with_lists, fileobj=output)
        writer.write([row_with_lists])
        result = output.getvalue().decode("utf-8")
        assert "['John','Jane','Bob']" in result  # noqa: S101
        assert "[10,20,30]" in result  # noqa: S101
        assert "['2024-10-01 10:00:00','2024-10-02 14:30:00']" in result  # noqa: S101

    def test_read_write_list_roundtrip(
        self,
        metadata_with_lists,
        row_with_lists,
    ):
        """Test roundtrip with lists."""

        buffer = io.BytesIO()
        writer = CSVWriter(metadata=metadata_with_lists, fileobj=buffer)
        writer.write([row_with_lists])
        buffer.seek(0)
        reader = CSVReader(
            fileobj=buffer,
            metadata=metadata_with_lists,
            delimiter=",",
            has_header=True,
        )
        rows = list(reader)
        assert len(rows) == 1  # noqa: S101
        read_row = rows[0]
        assert read_row[0] == 1  # noqa: S101
        assert read_row[1] == ["John", "Jane", "Bob"]  # noqa: S101
        assert read_row[2] == [10, 20, 30]  # noqa: S101
        assert read_row[3][0] == datetime.datetime(2024, 10, 1, 10, 0, 0)  # noqa: S101
        assert read_row[3][1] == datetime.datetime(2024, 10, 2, 14, 30, 0)  # noqa: S101


class TestCSVWriterProperties:
    """Тесты для свойств writer."""

    def test_columns_property(self, sample_metadata):

        writer = CSVWriter(metadata=sample_metadata)
        assert writer.columns == expected_columns  # noqa: S101

    def test_dtypes_property(self, sample_metadata):

        writer = CSVWriter(metadata=sample_metadata)
        assert writer.dtypes == expected_dtypes  # noqa: S101

    def test_num_columns(self, sample_metadata):

        writer = CSVWriter(metadata=sample_metadata)
        assert writer.num_columns == 17  # noqa: S101

    def test_tell_after_write(self, sample_metadata, sample_row):

        output = io.BytesIO()
        writer = CSVWriter(fileobj=output, metadata=sample_metadata)
        assert writer.tell() == 0  # noqa: S101
        writer.write([sample_row])
        assert writer.tell() > 0  # noqa: S101


class TestCSVReaderProperties:
    """Тесты для свойств reader."""

    def test_columns_property(self, reader: CSVReader):

        assert reader.columns == expected_columns  # noqa: S101

    def test_dtypes_property(self, reader: CSVReader):

        assert reader.dtypes == expected_dtypes  # noqa: S101

    def test_num_columns_property(self, reader: CSVReader):

        assert reader.num_columns == 17  # noqa: S101

    def test_num_rows_property(self, reader: CSVReader):

        assert reader.num_rows == 0  # noqa: S101
        list(reader)
        assert reader.num_rows == 1  # noqa: S101

    def test_read_row(self, reader: CSVReader, sample_row: list):

        row_gen = reader.read_row()
        row = next(row_gen)
        assert row is not None  # noqa: S101
        assert row[0] == sample_row[0]  # noqa: S101

    def test_to_rows(self, reader: CSVReader, sample_row: list):

        rows = list(reader.to_rows())
        assert len(rows) == 1  # noqa: S101
        assert rows[0][0] == sample_row[0]  # noqa: S101

    def test_tell(self, reader: CSVReader):

        pos = reader.tell()
        assert pos == 0  # noqa: S101
        list(reader)
        assert reader.tell() > 0  # noqa: S101


class TestCSVEdgeCases:
    """Тесты для граничных случаев."""

    def test_empty_rows(self):
        """Test writing and reading empty rows."""

        metadata = [{"id": "int"}, {"name": "str"}]
        buffer = io.BytesIO()
        buffer.seek(0)
        reader = CSVReader(
            fileobj=buffer,
            metadata=metadata,
            delimiter=",",
            has_header=True,
        )
        rows = list(reader)
        assert len(rows) == 0  # noqa: S101

    def test_none_values(self):
        """Test None values."""

        metadata = [{"id": "int"}, {"name": "str"}]
        row = (None, None)
        buffer = io.BytesIO()
        writer = CSVWriter(metadata=metadata, fileobj=buffer)
        writer.write([row])
        buffer.seek(0)
        reader = CSVReader(
            fileobj=buffer,
            metadata=metadata,
            delimiter=",",
            has_header=True,
        )
        rows = list(reader)
        assert len(rows) == 1  # noqa: S101
        assert rows[0][0] is None  # noqa: S101
        assert rows[0][1] is None  # noqa: S101

    def test_empty_strings(self):
        """Test empty strings."""

        metadata = [{"name": "str"}]
        row = ("",)
        buffer = io.BytesIO()
        writer = CSVWriter(metadata=metadata, fileobj=buffer)
        writer.write([row])
        buffer.seek(0)
        reader = CSVReader(
            fileobj=buffer,
            metadata=metadata,
            delimiter=",",
            has_header=True,
        )
        rows = list(reader)
        assert len(rows) == 1  # noqa: S101
        assert rows[0][0] is None  # noqa: S101

    def test_special_characters_in_strings(self):
        """Test strings with special characters."""

        metadata = [{"text": "str"}]
        row = ('Hello, "World"!',)
        buffer = io.BytesIO()
        writer = CSVWriter(metadata=metadata, fileobj=buffer)
        writer.write([row])
        buffer.seek(0)
        reader = CSVReader(
            fileobj=buffer,
            metadata=metadata,
            delimiter=",",
            has_header=True,
        )
        rows = list(reader)
        assert rows[0][0] == 'Hello, "World"!'  # noqa: S101

    def test_nested_lists(self):
        """Test nested lists."""

        metadata = [{"data": "list[list[int]]"}]
        row = ([[1, 2], [3, 4, 5]],)
        buffer = io.BytesIO()
        writer = CSVWriter(metadata=metadata, fileobj=buffer)
        writer.write([row])
        buffer.seek(0)
        reader = CSVReader(
            fileobj=buffer,
            metadata=metadata,
            delimiter=",",
            has_header=True,
        )
        rows = list(reader)
        assert len(rows) == 1  # noqa: S101
        assert rows[0][0] == [[1, 2], [3, 4, 5]]  # noqa: S101

    def test_none_and_empty_string_in_csv(self):
        """Test that None and empty string become empty fields."""

        metadata = [{"id": "int"}, {"value": "str"}, {"name": "str"}]
        row = (1, None, "")
        buffer = io.BytesIO()
        writer = CSVWriter(metadata=metadata, fileobj=buffer)
        writer.write([row])
        result = buffer.getvalue().decode("utf-8")
        lines = result.strip().split("\n")
        assert len(lines) == 2  # noqa: S101
        data_line = lines[1]
        assert data_line == "1,,"  # noqa: S101
        buffer.seek(0)
        reader = CSVReader(
            fileobj=buffer,
            metadata=metadata,
            delimiter=",",
            has_header=True,
        )
        rows = list(reader)
        assert len(rows) == 1  # noqa: S101
        assert rows[0][0] == 1  # noqa: S101
        assert rows[0][1] is None  # noqa: S101
        assert rows[0][2] is None  # noqa: S101

    def test_empty_field_becomes_none(self):
        """Test that empty field becomes None."""

        csv_data = b"id,name,age\n1,,30\n2,John,\n3,,"
        buffer = io.BytesIO(csv_data)
        metadata = [{"id": "int"}, {"name": "str"}, {"age": "int"}]
        reader = CSVReader(
            fileobj=buffer,
            metadata=metadata,
            delimiter=",",
            has_header=True,
        )
        rows = list(reader)
        assert len(rows) == 3  # noqa: S101
        assert rows[0][0] == 1  # noqa: S101
        assert rows[0][1] is None  # noqa: S101
        assert rows[0][2] == 30  # noqa: S101
        assert rows[1][0] == 2  # noqa: S101
        assert rows[1][1] == "John"  # noqa: S101
        assert rows[1][2] is None  # noqa: S101
        assert rows[2][0] == 3  # noqa: S101
        assert rows[2][1] is None  # noqa: S101
        assert rows[2][2] is None  # noqa: S101

    def test_none_in_various_positions(self):
        """Test None values at beginning, middle and end."""

        metadata = [
            {"col1": "int"},
            {"col2": "str"},
            {"col3": "int"},
            {"col4": "str"},
        ]
        rows = [
            (None, None, None, None),
            (None, "middle", 100, "end"),
            (1, None, 200, "end"),
            (2, "start", 300, None),
            (3, "start", None, "end"),
            (None, None, 400, "end"),
            (5, "start", None, None),
        ]
        buffer = io.BytesIO()
        writer = CSVWriter(metadata=metadata, fileobj=buffer)
        writer.write(rows)
        result = buffer.getvalue().decode("utf-8")
        lines = result.strip().split("\n")
        assert len(lines) == 8  # noqa: S101

        for i, line in enumerate(lines[1:], 1):
            fields = line.split(",")
            assert len(fields) == 4, (  # noqa: S101
                f"Row {i}: expected 4 fields, got {len(fields)}: {line}"
            )

        buffer.seek(0)
        reader = CSVReader(
            fileobj=buffer,
            metadata=metadata,
            delimiter=",",
            has_header=True,
        )
        read_rows = list(reader)
        assert len(read_rows) == 7  # noqa: S101

        for i, (original, read) in enumerate(zip(rows, read_rows)):
            for j in range(4):
                if original[j] is None:
                    assert read[j] is None, (  # noqa: S101
                        f"Row {i + 1}, col {j}: expected None, got {read[j]}"
                    )
                else:
                    assert read[j] == original[j], (  # noqa: S101
                        f"Row {i + 1}, col {j}: "
                        f"expected {original[j]}, got {read[j]}"
                    )

    def test_special_characters_comma_and_quote(self):
        """Test fields with commas, single quotes, and double quotes."""

        metadata = [{"text": "str"}]
        rows = [
            ("Hello, world",),
            ("It's a beautiful day",),
            ('He said "Hello" to me',),
            ("It's a \"beautiful\" day, isn't it?",),
            ('Field with , comma and "quotes"',),
            ("Field with 'single' quotes",),
            (",,",),
            ('""',),
            ("''",),
            ('"Hello, world"',),
            ("'Hello, world'",),
            ("Field with, multiple, commas",),
            ("Field with \"double\" and 'single' quotes, and comma",),
        ]
        buffer = io.BytesIO()
        writer = CSVWriter(metadata=metadata, fileobj=buffer)
        writer.write(rows)
        result = buffer.getvalue().decode("utf-8")
        lines = result.strip().split("\n")
        assert len(lines) == len(rows) + 1  # noqa: S101
        buffer.seek(0)
        reader = CSVReader(
            fileobj=buffer,
            metadata=metadata,
            delimiter=",",
            has_header=True,
        )
        read_rows = list(reader)
        assert len(read_rows) == len(rows)  # noqa: S101

        for i, (original, read) in enumerate(zip(rows, read_rows)):
            assert read[0] == original[0], (  # noqa: S101
                f"Row {i + 1}: expected {original[0]!r}, got {read[0]!r}"
            )

        for i, line in enumerate(lines[1:], 1):
            if "," in rows[i - 1][0] and not rows[i - 1][0].startswith('"'):
                assert line.startswith('"') or '""' in line, (  # noqa: S101
                    f"Row {i}: field with comma should be quoted: {line}"
                )

    def test_multiple_columns_with_special_chars(self):
        """Test multiple columns with commas and quotes."""

        metadata = [
            {"id": "int"},
            {"name": "str"},
            {"description": "str"},
            {"tags": "list[str]"},
        ]
        rows = [
            (1, "Smith, John", 'He said "Hello"', ["python", "csv", "test"]),
            (2, "O'Connor", "It's a test, with comma", ["data", "analysis"]),
            (3, "Do, Jane", 'Field "quotes" and , comma', ["rust", "python"]),
            (4, "Williams", "No special chars", ["simple"]),
            (5, "Lu, 'Tom'", "Mixed \"quote\" & 'apostroph', comma", ["test"]),
        ]
        buffer = io.BytesIO()
        writer = CSVWriter(metadata=metadata, fileobj=buffer)
        writer.write(rows)
        buffer.seek(0)
        reader = CSVReader(
            fileobj=buffer,
            metadata=metadata,
            delimiter=",",
            has_header=True,
        )
        read_rows = list(reader)
        assert len(read_rows) == len(rows)  # noqa: S101

        for i, (original, read) in enumerate(zip(rows, read_rows)):
            assert read[0] == original[0]  # noqa: S101
            assert read[1] == original[1]  # noqa: S101
            assert read[2] == original[2]  # noqa: S101
            assert read[3] == original[3]  # noqa: S101

    def test_bytes_handling(self):
        """Test handling of bytes data
        (PostgreSQL bytea and SQL Server binary)."""

        metadata = [
            {"id": "int"},
            {"binary_data": "bytes"},
            {"hex_data": "bytes"},
        ]
        test_bytes = b'\xde\xad\xbe\xef'
        test_hex = b'Hello, World!'
        rows = [
            (1, test_bytes, test_hex),
            (2, b'\x00\x01\x02\x03', b'Simple bytes'),
            (3, b'', b''),
            (4, b'\x89PNG\r\n\x1a\n', b'PNG header'),
        ]
        buffer = io.BytesIO()
        writer = CSVWriter(metadata=metadata, fileobj=buffer)
        writer.write(rows)
        result = buffer.getvalue().decode("utf-8")
        assert "\\xdeadbeef" in result  # noqa: S101
        assert "\\x48656c6c6f2c20576f726c6421" in result  # noqa: S101
        assert "\\x" in result  # noqa: S101
        buffer.seek(0)
        reader = CSVReader(
            fileobj=buffer,
            metadata=metadata,
            delimiter=",",
            has_header=True,
        )
        read_rows = list(reader)
        assert len(read_rows) == 4  # noqa: S101

        for i, (original, read) in enumerate(zip(rows, read_rows)):
            assert read[0] == original[0], f"Row {i+1}: id mismatch"  # noqa: S101
            assert isinstance(  # noqa: S101
                read[1], bytes), f"Row {i+1}: binary_data should be bytes"
            assert read[1] == original[1], f"Row {i+1}: binary_data mismatch"  # noqa: S101

            assert isinstance(  # noqa: S101
                read[2], bytes), f"Row {i+1}: hex_data should be bytes"
            assert read[2] == original[2], f"Row {i+1}: hex_data mismatch"  # noqa: S101

        assert read_rows[2][1] == b''  # noqa: S101
        assert read_rows[2][2] == b''  # noqa: S101
        rows_with_none = [
            (5, None, None),
        ]
        buffer2 = io.BytesIO()
        writer2 = CSVWriter(metadata=metadata, fileobj=buffer2)
        writer2.write(rows_with_none)
        buffer2.seek(0)
        reader2 = CSVReader(
            fileobj=buffer2,
            metadata=metadata,
            delimiter=",",
            has_header=True,
        )
        read_none_rows = list(reader2)
        assert len(read_none_rows) == 1  # noqa: S101
        assert read_none_rows[0][1] is None  # noqa: S101
        assert read_none_rows[0][2] is None  # noqa: S101


if __name__ == "__main__":
    pytest.main([__file__, "-svv"])
