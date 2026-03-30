#!/usr/bin/env python3
"""
Open Data CSV extraction — SQL Server direct.
Replaces the PostgreSQL-based refresh_github_extract.bash pipeline.
Queries x_ tables from TMSPublicExtract and outputs CSVs matching
the exact production headers (lowercase column names, same column order).

Handles formatting differences between SQL Server and PostgreSQL:
- Booleans (bit): True/False → 1/0
- Timestamps: adds US Eastern timezone offset (-04/-05)
- Empty strings: quoted as "" to distinguish from NULL
- Microseconds: trimmed trailing zeros (.937000 → .937)

Usage:
  extract_opendata.py --server HOST --database DB [--git-push] [--output-dir DIR]

Requires --server and --database (or env vars OPENDATA_SERVER, OPENDATA_DATABASE).
"""

import argparse
import datetime
import os
import subprocess
import sys
import pyodbc
from dateutil import tz

# --- Configuration ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUTPUT_DIR = os.path.join(SCRIPT_DIR, "..", "data")
EASTERN = tz.gettz("America/New_York")

# Subquery fragments reused across tables
ACCESSIONED_OBJECTS = "SELECT DISTINCT objectID FROM x_objects WHERE accessioned = 1"
ACCESSIONED_CONSTITUENTS = (
    f"SELECT DISTINCT constituentID FROM x_objects_constituents "
    f"WHERE objectID IN ({ACCESSIONED_OBJECTS})"
)

# --- Table definitions ---
# Each entry: (csv_filename, columns_as_csv_headers, sql_query)
# Columns listed in exact production CSV header order (lowercase).

TABLES = [
    (
        "constituents",
        ["constituentid", "uuid", "ulanid", "preferreddisplayname", "forwarddisplayname",
         "lastname", "displaydate", "artistofngaobject", "beginyear", "endyear",
         "visualbrowsertimespan", "nationality", "visualbrowsernationality",
         "constituenttype", "wikidataid"],
        f"""SELECT constituentID, uuid, ULANID, preferredDisplayName, forwardDisplayName,
                   lastName, displayDate, artistOfNGAObject, beginYear, endYear,
                   visualBrowserTimeSpan, nationality, visualBrowserNationality,
                   constituentType, wikidataID
            FROM x_constituents
            WHERE constituentID IN ({ACCESSIONED_CONSTITUENTS})
            ORDER BY constituentID"""
    ),
    (
        "constituents_altnames",
        ["altnameid", "constituentid", "lastname", "displayname",
         "forwarddisplayname", "nametype"],
        # forwardDisplayName: was NULL in the old PostgreSQL pipeline, now populated from source
        f"""SELECT altNameID, constituentID, lastName, displayName,
                   forwardDisplayName, nameType
            FROM x_constituents_altnames
            WHERE constituentID IN ({ACCESSIONED_CONSTITUENTS})
            ORDER BY altNameID"""
    ),
    (
        "constituents_text_entries",
        ["constituentid", "text", "texttype", "year"],
        "SELECT constituentID, [text], textType, [year] FROM x_constituents_text_entries WHERE textType = 'bibliography' ORDER BY constituentID"
    ),
    (
        "locations",
        ["locationid", "site", "room", "publicaccess", "description", "unitposition"],
        "SELECT locationID, site, room, publicAccess, description, unitPosition FROM x_locations ORDER BY locationID"
    ),
    (
        "media_items",
        ["mediaid", "mediatype", "title", "description", "duration", "language",
         "thumbnailurl", "playurl", "downloadurl", "keywords", "tags", "imageurl",
         "presentationdate", "releasedate", "lastmodified"],
        """SELECT mediaID, mediaType, title, description, duration, language,
                  thumbnailURL, playURL, downloadURL, keywords, tags, imageURL,
                  presentationDate, releaseDate, lastModified
           FROM x_media_items
           ORDER BY mediaID"""
    ),
    (
        "media_relationships",
        ["mediaid", "relatedid", "relatedentity"],
        "SELECT mediaID, relatedID, relatedEntity FROM x_media_relationships ORDER BY mediaID, relatedID"
    ),
    (
        "object_associations",
        ["parentobjectid", "childobjectid", "relationship"],
        f"""SELECT DISTINCT a.parentObjectID, a.childObjectID, a.relationship
            FROM x_object_associations a
            JOIN x_objects o ON o.objectID IN (a.parentObjectID, a.childObjectID)
            WHERE o.accessioned = 1
            ORDER BY a.parentObjectID, a.childObjectID"""
    ),
    (
        "objects",
        ["objectid", "uuid", "accessioned", "accessionnum", "locationid", "title",
         "displaydate", "beginyear", "endyear", "visualbrowsertimespan", "medium",
         "dimensions", "inscription", "markings", "attributioninverted", "attribution",
         "provenancetext", "creditline", "classification", "subclassification",
         "visualbrowserclassification", "parentid", "isvirtual", "departmentabbr",
         "portfolio", "series", "volume", "watermarks", "lastdetectedmodification",
         "wikidataid", "customprinturl"],
        """SELECT o.objectID, o.uuid, o.accessioned, o.accessionNum, o.locationID, o.title,
                  o.displayDate, o.beginYear, o.endYear, o.visualBrowserTimeSpan,
                  o.medium, o.dimensions, o.inscription, o.markings,
                  o.attributionInverted, o.attribution, o.provenanceText, o.creditLine,
                  o.classification, o.subClassification, o.visualBrowserClassification,
                  o.parentID, o.isVirtual, o.departmentAbbr, o.portfolio, o.series,
                  o.volume, o.watermarks, o.lastDetectedModification, o.wikidataID,
                  p.URL AS customPrintURL
           FROM x_objects o
           LEFT JOIN x_objects_customprint_urls p ON p.TMSObjectID = o.objectID
           WHERE o.accessioned = 1
           ORDER BY o.objectID"""
    ),
    (
        "alternative_identifiers",
        ["uuid", "idschemelabel", "identifier"],
        "SELECT uuid, idSchemeLabel, identifier FROM x_alternative_identifiers WHERE idSchemeLabel <> 'Old CMS Object ID' ORDER BY uuid, idSchemeLabel"
    ),
    (
        "objects_constituents",
        ["objectid", "constituentid", "displayorder", "roletype", "role", "prefix",
         "suffix", "displaydate", "beginyear", "endyear", "country", "zipcode"],
        f"""SELECT objectID, constituentID, displayOrder, roleType, role, prefix,
                   suffix, displayDate, beginYear, endYear, country, zipCode
            FROM x_objects_constituents
            WHERE objectID IN ({ACCESSIONED_OBJECTS})
            ORDER BY objectID, constituentID, displayOrder, roleType, role, prefix, suffix, displayDate, beginYear, endYear, country, zipCode"""
    ),
    (
        "objects_dimensions",
        ["objectid", "element", "dimensiontype", "dimension", "unitname"],
        f"""SELECT d.objectID, d.element, d.dimensionType, d.dimension, d.unitName
            FROM x_objects_dimensions d
            JOIN x_objects o ON o.objectID = d.objectID
            WHERE o.accessioned = 1
            ORDER BY d.objectID, d.element, d.dimensionType"""
    ),
    (
        "objects_historical_data",
        ["datatype", "objectid", "displayorder", "forwardtext", "invertedtext",
         "remarks", "effectivedate"],
        f"""SELECT h.dataType, h.objectID, h.displayOrder, h.forwardText,
                   h.invertedText, h.remarks, h.effectiveDate
            FROM x_objects_historical_data h
            JOIN x_objects o ON o.objectID = h.objectID
            WHERE o.accessioned = 1
            ORDER BY h.objectID, h.dataType, h.displayOrder"""
    ),
    (
        "objects_terms",
        ["termid", "objectid", "termtype", "term", "visualbrowsertheme",
         "visualbrowserstyle"],
        f"""SELECT t.termID, t.objectID, t.termType, t.term,
                   t.visualBrowserTheme, t.visualBrowserStyle
            FROM x_objects_terms t
            JOIN x_objects o ON o.objectID = t.objectID
            WHERE o.accessioned = 1
            ORDER BY t.termID, t.objectID"""
    ),
    (
        "objects_text_entries",
        ["objectid", "text", "texttype", "year"],
        f"""SELECT t.objectID, t.[text], t.textType, t.[year]
            FROM x_objects_text_entries t
            JOIN x_objects o ON o.objectID = t.objectID
            WHERE o.accessioned = 1
              AND t.textType IN ('bibliography','documentary_labels_inscriptions',
                  'exhibition_history','exhibition_history_footnote',
                  'inscription_footnote','lifetime_exhibition','other_collections')
            ORDER BY t.objectID, t.textType, t.[year], t.fingerprint"""
    ),
    (
        "preferred_locations",
        ["locationkey", "locationtype", "description", "ispublicvenue",
         "mapimageurl", "mapshapetype", "mapshapecoords", "partof"],
        """SELECT locationKey, locationType, description, isPublicVenue,
                  mapImageURL, mapShapeType, mapShapeCoords, partOf
           FROM x_preferred_locations
           ORDER BY locationKey"""
    ),
    (
        "preferred_locations_tms_locations",
        ["preferredlocationkey", "tmslocationid"],
        "SELECT preferredLocationKey, tmsLocationID FROM x_preferred_locations_tms_locations ORDER BY preferredLocationKey, tmsLocationID"
    ),
    (
        "published_images",
        ["uuid", "iiifurl", "iiifthumburl", "viewtype", "sequence", "width",
         "height", "maxpixels", "openaccess", "created", "modified",
         "depictstmsobjectid", "assistivetext"],
        """SELECT uuid,
                  CAST('https://api.nga.gov/iiif/' + uuid AS VARCHAR(512)) AS iiifURL,
                  CAST('https://api.nga.gov/iiif/' + uuid + '/full/!200,200/0/default.jpg' AS VARCHAR(512)) AS iiifThumbURL,
                  viewType, sequence, width, height, maxPixels,
                  CASE WHEN obj_rightsType = 'Open Access' THEN 1 ELSE 0 END AS openAccess,
                  created, modified, depictsTMSObjectID, assistiveText
           FROM x_published_images
           WHERE depictsTMSObjectID IS NOT NULL
             AND ri_photoCredit IS NULL
             AND viewType IN ('primary','alternate')
             AND COALESCE(ri_isDetail,'false') = 'false'
           ORDER BY uuid"""
    ),
]


def connect(server, database):
    connstr = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server};DATABASE={database};"
        f"Trusted_Connection=yes;TrustServerCertificate=yes"
    )
    return pyodbc.connect(connstr)


def format_datetime(dt):
    """Format datetime to match PostgreSQL timestamptz output.
    SQL Server datetime has no timezone — treat as US Eastern and add offset."""
    aware = dt.replace(tzinfo=EASTERN)
    offset = aware.strftime("%z")  # e.g. "-0400" or "-0500"
    offset_short = offset[:3]      # e.g. "-04" or "-05"

    if dt.microsecond:
        # Trim trailing zeros from fractional seconds: .937000 → .937
        frac = f"{dt.microsecond:06d}".rstrip("0")
        ts = dt.strftime("%Y-%m-%d %H:%M:%S") + "." + frac
    else:
        ts = dt.strftime("%Y-%m-%d %H:%M:%S")

    return ts + offset_short


def format_value(v):
    """Convert a SQL Server value to match PostgreSQL COPY CSV output."""
    if v is None:
        return None  # sentinel — will become empty unquoted field
    if isinstance(v, bool):
        return str(int(v))
    if isinstance(v, datetime.datetime):
        return format_datetime(v)
    s = str(v)
    return s


# CSV field formatting that matches PostgreSQL COPY behavior:
# - NULL → empty between commas (no quotes)
# - empty string → "" (quoted)
# - strings with comma/quote/newline → quoted
# - everything else → unquoted

def needs_quoting(s):
    return '"' in s or ',' in s or '\n' in s or '\r' in s


def write_csv_row(f, values):
    """Write one CSV row matching PostgreSQL COPY CSV format."""
    parts = []
    for v in values:
        if v is None:
            parts.append("")
        elif v == "":
            parts.append('""')
        elif needs_quoting(v):
            parts.append('"' + v.replace('"', '""') + '"')
        else:
            parts.append(v)
    f.write(",".join(parts) + "\n")


def extract_table(conn, name, headers, query, output_dir):
    """Run query and write CSV. Returns row count."""
    path = os.path.join(output_dir, f"{name}.csv")
    cur = conn.cursor()
    cur.execute(query)

    count = 0
    with open(path, "w", encoding="utf-8") as f:
        # Header row — plain comma-separated, no quoting needed
        f.write(",".join(headers) + "\n")
        for row in cur:
            formatted = [format_value(v) for v in row]
            write_csv_row(f, formatted)
            count += 1
    cur.close()
    return count


def git_push(output_dir):
    """Stage CSVs, commit with date, and push."""
    repo_dir = subprocess.check_output(
        ["git", "rev-parse", "--show-toplevel"], cwd=output_dir
    ).decode().strip()
    def run(cmd):
        subprocess.check_call(cmd, cwd=repo_dir)
    run(["git", "pull"])
    run(["git", "add", output_dir])
    # Skip commit+push if nothing changed
    if subprocess.call(["git", "diff", "--cached", "--quiet"], cwd=repo_dir) == 0:
        print("Git: no changes to commit")
        return
    msg = datetime.datetime.now().strftime("%Y-%m-%d") + " data export"
    run(["git", "commit", "-m", msg])
    run(["git", "push"])
    print(f"Git: committed and pushed ({msg})")


def parse_args():
    p = argparse.ArgumentParser(description="Open Data CSV extraction from SQL Server")
    p.add_argument("--server", default=os.environ.get("OPENDATA_SERVER"),
                   help="SQL Server hostname (or set OPENDATA_SERVER)")
    p.add_argument("--database", default=os.environ.get("OPENDATA_DATABASE"),
                   help="Database name (or set OPENDATA_DATABASE)")
    p.add_argument("--output-dir", help="Output directory (default: ../data)")
    p.add_argument("--git-push", action="store_true",
                   help="Git add, commit, and push after extraction")
    args = p.parse_args()
    if not args.server or not args.database:
        p.error("--server and --database are required (or set OPENDATA_SERVER / OPENDATA_DATABASE)")
    return args


def main():
    args = parse_args()

    server = args.server
    database = args.database
    output_dir = args.output_dir or DEFAULT_OUTPUT_DIR

    os.makedirs(output_dir, exist_ok=True)

    print(f"Connecting to {server}/{database} ...")
    conn = connect(server, database)
    print("Connected.\n")

    try:
        total = 0
        for name, headers, query in TABLES:
            count = extract_table(conn, name, headers, query, output_dir)
            total += count
            print(f"  {name}: {count:,} rows")
    finally:
        conn.close()

    print(f"\nDone. {len(TABLES)} tables, {total:,} total rows.")
    print(f"Output: {output_dir}")

    if args.git_push:
        git_push(output_dir)


if __name__ == "__main__":
    main()
