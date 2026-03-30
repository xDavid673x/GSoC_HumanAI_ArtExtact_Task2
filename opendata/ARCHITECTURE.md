# Architecture

## Overview

The NGA Open Data pipeline extracts collection data from the NGA's TMS (The Museum System) public extract database and publishes it as CSV files to this GitHub repository.

## Data Flow

```
TMSPublicExtract (SQL Server)
    x_ tables (permanent, consumer-facing)
        |
        v
extract_opendata.py
    - Connects via ODBC
    - Queries 17 x_ tables directly
    - Formats output to match legacy CSV conventions
        |
        v
    data/*.csv (17 CSV files)
        |
        v (optional --git-push)
    git add, commit, push → GitHub
```

## Previous Architecture

The original pipeline used a PostgreSQL intermediary:

```
TMS → PostgreSQL (vm-webdb-tdp) → psql COPY → CSV → GitHub
```

`refresh_github_extract.bash` ran `psql` queries defined in `tables.sql`, exported via `COPY ... TO STDOUT WITH CSV HEADER`, then committed and pushed. This required maintaining a separate PostgreSQL database as a staging layer.

## Current Architecture

`extract_opendata.py` queries the TMS public extract SQL Server database directly, eliminating the PostgreSQL intermediary. The x_ tables in TMSPublicExtract are the same tables that fed the PostgreSQL staging database, so the output is equivalent.

### Key Design Decisions

- **Direct SQL Server access**: Removes the PostgreSQL dependency and simplifies the data path.
- **CSV format compatibility**: Output matches the legacy PostgreSQL `COPY CSV` format exactly — same headers, same quoting rules, same NULL representation — so downstream consumers are unaffected.
- **Timezone handling**: SQL Server datetimes have no timezone. The script treats them as US Eastern and appends the correct UTC offset (`-04` or `-05`) to match the PostgreSQL `timestamptz` output.
- **Authentication**: Uses a service account. No passwords are stored in the script or repository.

### What the Script Does

1. Connects to SQL Server via pyodbc (ODBC Driver 18)
2. Iterates over 17 table definitions (name, headers, SQL query)
3. For each table: executes the query, formats values, writes CSV
4. Optionally stages, commits, and pushes to git

```
extract_opendata.py --server HOST --database DB [--git-push] [--output-dir DIR]
```

### Formatting Rules

| Type | PostgreSQL COPY | This script |
|------|----------------|-------------|
| NULL | empty between commas | empty between commas |
| Empty string | `""` | `""` |
| Boolean (bit) | `1`/`0` | `1`/`0` |
| Timestamp | `2024-01-01 12:00:00-05` | `2024-01-01 12:00:00-05` |
| Microseconds | `.937000` | `.937` (trailing zeros trimmed) |
| Comma/quote in string | double-quoted, `""` escaped | double-quoted, `""` escaped |

## Tables Extracted

17 tables covering objects, constituents, locations, images, and relationships. See `documentation/Data Dictionary.txt` for column-level documentation.

## Testing

Tests are in `tests/` and require `pytest` (`pip install pytest`).

- **Unit tests** (`test_csv_formatting.py`): CSV formatting, value conversion, header validation. No database needed.
- **Integration tests** (`test_database.py`): Runs all 17 queries against the database, validates column counts, row counts, data constraints (e.g., openaccess values are only 0 or 1). Requires database access. Tests auto-skip gracefully if no database is available.

Run all tests:
```
pytest tests/ -v
```

Run unit tests only (no database):
```
pytest tests/test_csv_formatting.py -v
```

## Deployment

The extraction runs on a scheduled cron job. See the operations team for the current schedule and service account configuration.
