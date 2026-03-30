"""Unit tests for CSV formatting functions in extract_opendata.py."""
import datetime
import io
import os
import sys

import pytest

# Add scripts/ to path so we can import the extraction module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
import extract_opendata as ext


# --- format_datetime ---

class TestFormatDatetime:
    def test_winter_offset(self):
        """January datetime should get -05 (EST)."""
        dt = datetime.datetime(2024, 1, 15, 10, 30, 0)
        assert ext.format_datetime(dt).endswith("-05")

    def test_summer_offset(self):
        """July datetime should get -04 (EDT)."""
        dt = datetime.datetime(2024, 7, 15, 10, 30, 0)
        assert ext.format_datetime(dt).endswith("-04")

    def test_microseconds_trimmed(self):
        """.937000 should become .937"""
        dt = datetime.datetime(2024, 1, 1, 0, 0, 0, 937000)
        result = ext.format_datetime(dt)
        assert ".937-" in result
        assert ".937000" not in result

    def test_microseconds_single_trailing(self):
        """.100000 should become .1"""
        dt = datetime.datetime(2024, 1, 1, 0, 0, 0, 100000)
        result = ext.format_datetime(dt)
        assert ".1-" in result

    def test_no_microseconds(self):
        """No fractional part when microseconds=0."""
        dt = datetime.datetime(2024, 1, 1, 12, 0, 0, 0)
        result = ext.format_datetime(dt)
        # Extract seconds portion (after last ":" up to offset)
        assert result == "2024-01-01 12:00:00-05"

    def test_full_format(self):
        dt = datetime.datetime(2024, 6, 15, 14, 30, 45, 123000)
        result = ext.format_datetime(dt)
        assert result == "2024-06-15 14:30:45.123-04"


# --- format_value ---

class TestFormatValue:
    def test_none(self):
        assert ext.format_value(None) is None

    def test_true(self):
        assert ext.format_value(True) == "1"

    def test_false(self):
        assert ext.format_value(False) == "0"

    def test_integer(self):
        assert ext.format_value(42) == "42"

    def test_float(self):
        assert ext.format_value(3.14) == "3.14"

    def test_string(self):
        assert ext.format_value("hello") == "hello"

    def test_empty_string(self):
        assert ext.format_value("") == ""

    def test_datetime(self):
        dt = datetime.datetime(2024, 1, 1, 0, 0, 0)
        result = ext.format_value(dt)
        assert "2024-01-01" in result
        assert "-05" in result


# --- needs_quoting ---

class TestNeedsQuoting:
    def test_comma(self):
        assert ext.needs_quoting("a,b") is True

    def test_double_quote(self):
        assert ext.needs_quoting('say "hi"') is True

    def test_newline(self):
        assert ext.needs_quoting("line1\nline2") is True

    def test_carriage_return(self):
        assert ext.needs_quoting("line1\rline2") is True

    def test_plain_text(self):
        assert ext.needs_quoting("plain text") is False

    def test_empty(self):
        assert ext.needs_quoting("") is False


# --- write_csv_row ---

class TestWriteCsvRow:
    def _write(self, values):
        buf = io.StringIO()
        ext.write_csv_row(buf, values)
        return buf.getvalue()

    def test_none_becomes_empty(self):
        assert self._write([None]) == "\n"

    def test_empty_string_quoted(self):
        assert self._write([""]) == '""\n'

    def test_string_with_comma(self):
        assert self._write(["a,b"]) == '"a,b"\n'

    def test_embedded_quotes(self):
        assert self._write(['say "hi"']) == '"say ""hi"""\n'

    def test_mixed_row(self):
        result = self._write([None, "", "plain", "a,b"])
        assert result == ',"",' + 'plain,"a,b"\n'

    def test_all_none(self):
        result = self._write([None, None, None])
        assert result == ",,\n"

    def test_plain_unquoted(self):
        result = self._write(["hello", "world"])
        assert result == "hello,world\n"


# --- TABLES list structure ---

class TestTablesDefinition:
    def test_table_count(self):
        assert len(ext.TABLES) > 0

    def test_each_entry_is_triple(self):
        for entry in ext.TABLES:
            assert len(entry) == 3, f"Entry {entry[0]} should be (name, headers, query)"

    def test_no_duplicate_names(self):
        names = [t[0] for t in ext.TABLES]
        assert len(names) == len(set(names))

    def test_headers_are_lowercase(self):
        for name, headers, _ in ext.TABLES:
            for h in headers:
                assert h == h.lower(), f"{name}: header '{h}' not lowercase"

    def test_published_images_has_openaccess(self):
        for name, headers, _ in ext.TABLES:
            if name == "published_images":
                assert "openaccess" in headers
                break
        else:
            pytest.fail("published_images not found in TABLES")
