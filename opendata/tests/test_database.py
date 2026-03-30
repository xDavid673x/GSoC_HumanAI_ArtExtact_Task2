"""Integration tests requiring a SQL Server connection.

Runs read-only queries against TMSPublicExtract to verify extraction
queries execute correctly. Skips automatically if DB is unreachable.

Connection uses Kerberos via service account credentials from
/usr/local/nga/etc/tmspublicextract.conf (see conftest.py).

Usage:
  pytest tests/test_database.py -v
  pytest tests/test_database.py -v --server ap-tmstst-db --database TMSPublicExtract
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
import extract_opendata as ext

# db fixture is provided by conftest.py


# --- Connection ---

class TestConnection:
    def test_connects(self, db):
        cur = db.cursor()
        cur.execute("SELECT 1")
        assert cur.fetchone()[0] == 1
        cur.close()


# --- All 17 queries execute without error ---

@pytest.mark.parametrize("name,headers,query", ext.TABLES, ids=[t[0] for t in ext.TABLES])
class TestQueryExecution:
    def test_query_runs(self, db, name, headers, query):
        """Query executes without SQL error."""
        cur = db.cursor()
        cur.execute(query)
        cur.fetchone()
        cur.close()

    def test_column_count_matches_headers(self, db, name, headers, query):
        """Result set column count matches headers list."""
        cur = db.cursor()
        cur.execute(query)
        row = cur.fetchone()
        if row is not None:
            assert len(row) == len(headers), (
                "{}: query returns {} cols but headers has {}".format(name, len(row), len(headers))
            )
        cur.close()

    def test_returns_rows(self, db, name, headers, query):
        """Query returns at least one row."""
        cur = db.cursor()
        cur.execute(query)
        row = cur.fetchone()
        assert row is not None, "{}: query returned 0 rows".format(name)
        cur.close()


# --- Published images openaccess column ---

class TestOpenAccess:
    def test_only_zero_or_one(self, db):
        """openaccess column produces only 0 or 1."""
        cur = db.cursor()
        cur.execute("""
            SELECT DISTINCT CASE WHEN obj_rightsType = 'Open Access' THEN 1 ELSE 0 END
            FROM x_published_images
            WHERE depictsTMSObjectID IS NOT NULL
              AND ri_photoCredit IS NULL
              AND viewType IN ('primary','alternate')
              AND COALESCE(ri_isDetail,'false') = 'false'
        """)
        values = {row[0] for row in cur.fetchall()}
        cur.close()
        assert values <= {0, 1}

    def test_has_open_access_rows(self, db):
        """At least some images are open access."""
        cur = db.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM x_published_images
            WHERE obj_rightsType = 'Open Access'
              AND depictsTMSObjectID IS NOT NULL
              AND ri_photoCredit IS NULL
              AND viewType IN ('primary','alternate')
              AND COALESCE(ri_isDetail,'false') = 'false'
        """)
        count = cur.fetchone()[0]
        cur.close()
        assert count > 0, "No open access images found"

    def test_has_restricted_rows(self, db):
        """At least some images are rights restricted."""
        cur = db.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM x_published_images
            WHERE (obj_rightsType IS NULL OR obj_rightsType <> 'Open Access')
              AND depictsTMSObjectID IS NOT NULL
              AND ri_photoCredit IS NULL
              AND viewType IN ('primary','alternate')
              AND COALESCE(ri_isDetail,'false') = 'false'
        """)
        count = cur.fetchone()[0]
        cur.close()
        assert count > 0, "No rights-restricted images found"


# --- Row count sanity (order-of-magnitude checks) ---

class TestRowCounts:
    """Verify row counts are in expected ranges for a populated database."""

    EXPECTED_MINIMUMS = {
        "objects": 100000,
        "constituents": 10000,
        "published_images": 50000,
        "objects_constituents": 100000,
        "objects_terms": 100000,
        "locations": 1000,
    }

    @pytest.mark.parametrize("name,headers,query", ext.TABLES, ids=[t[0] for t in ext.TABLES])
    def test_minimum_rows(self, db, name, headers, query):
        if name not in self.EXPECTED_MINIMUMS:
            pytest.skip("No minimum defined for {}".format(name))
        # Strip ORDER BY — SQL Server disallows it in derived tables
        import re
        q = re.sub(r'\s+ORDER BY\s+[^)]+$', '', query, flags=re.IGNORECASE)
        cur = db.cursor()
        cur.execute("SELECT COUNT(*) FROM ({}) AS q".format(q))
        count = cur.fetchone()[0]
        cur.close()
        minimum = self.EXPECTED_MINIMUMS[name]
        assert count >= minimum, "{}: {} rows < expected minimum {}".format(name, count, minimum)


# --- No internal columns leak ---

class TestNoInternalColumns:
    """Verify internal columns don't appear in query results."""

    INTERNAL_COLS = {"_row_updated_at", "_row_hash", "fingerprint", "ri_photocredit", "ri_isdetail"}

    @pytest.mark.parametrize("name,headers,query", ext.TABLES, ids=[t[0] for t in ext.TABLES])
    def test_no_internal_columns_in_headers(self, name, headers, query):
        leaked = self.INTERNAL_COLS & set(headers)
        assert not leaked, "{}: internal columns in headers: {}".format(name, leaked)

    @pytest.mark.parametrize("name,headers,query", ext.TABLES, ids=[t[0] for t in ext.TABLES])
    def test_no_internal_columns_in_results(self, db, name, headers, query):
        """Check result set column names don't include internal fields."""
        cur = db.cursor()
        cur.execute(query)
        col_names = {col[0].lower() for col in cur.description}
        cur.close()
        leaked = self.INTERNAL_COLS & col_names
        assert not leaked, "{}: internal columns in result set: {}".format(name, leaked)
