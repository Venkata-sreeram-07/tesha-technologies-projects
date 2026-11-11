"""
Dataproc Serverless PySpark job: Epic Signal metric extract -> cleaned, pivoted
BigQuery tables, with a duplicate-metric data-quality audit.

Input  : SignalDownload_*.xlsx / native Google Sheets in a Drive folder.
         Long format, one row per (EMP CID, SER CID, Metric):
         EMP CID | SER CID | Clinician Name | Clinician Type | Login Service Area |
         Login Department | Specialty | User Type | Reporting Period Start Date |
         Reporting Period End Date | Metric | Numerator | Denominator | Value | Metric ID

FLOW   : Gdrive -> pivot -> clean -> transform -> ONE final BigQuery table.
         The source long/pivoted data is NEVER landed in BigQuery. Each file is
         processed in memory and only the transformed result is appended.

Output : exactly five BigQuery tables, no monthly source tables.
  A. audit_table                    one row per file/sheet per run: what was
                                    found at the source (size, md5, status ...)
  B. metadata_table                 one row per sheet per run: data-quality
                                    profile (nulls, duplicates, bad chars ...)
  C. signal_metric_duplicate_detail one row per (source file, duplicated metric)
  D. signal_metric_duplicate_summary one row per source file (duplicate stats)
  E. BQ_Final_Processed_data        THE target. One row per clinician-period,
                                    appended file by file: identifiers, dimensions,
                                    reporting period, and the derived metrics
                                    (appointment volume, chart-closure rates, notes
                                    time, after-hours hours, LOS mix).

----------------------------------------------------------------------------
WHAT THIS JOB DOES, IN ORDER
----------------------------------------------------------------------------
STEP 0  SCHEMA INFERENCE - .xlsx parsed with the standard library; cell types
        come from the file, DATE cells resolved from xl/styles.xml. TYPE_OVERRIDES
        pins ids to INT64 and numerator/value to FLOAT64.

STEP 1  DUPLICATE AUDIT (on the raw long rows) -> duplicate_detail / _summary.
        A duplicate is the same metric appearing more than once for one
        (emp_cid, ser_cid); the audit records how many ser_cids are affected and
        whether their repeated rows disagree on the measure (a real conflict).

STEP 2/3 TRANSFORM (Spark SQL, see TRANSFORM_SQL / --transform-sql-gcs)
        The long rows for the file are standardised (CPT 9920x/9921x level-of-
        service metrics mapped to New/Established levels), the metrics the output
        needs are pivoted with SUM(numerator), and the derived columns are
        computed with the same null rules as the original BigQuery script:
        numeric NULL/blank -> 0, string NULL/blank -> 'N/A', dates stay NULL.
        This is the Spark-SQL equivalent of the attached BigQuery transformation.

STEP 4  LOAD -> BQ_Final_Processed_data (append). The table is created on first
        use with the derived schema (partitioned by month_year). Re-processing a
        modified file first DELETEs that file's previous rows (idempotent), then
        appends. Duplicate audit tables are appended, keyed by run_id.

----------------------------------------------------------------------------
ARGUMENTS (UI "Arguments" field, one per entry)
----------------------------------------------------------------------------
  --folder-id=<DRIVE_FOLDER_ID>    default: DEFAULT_FOLDER_ID
  --file-id=<DRIVE_FILE_ID>        process a single file instead of a folder
  --sheet=<name|index|auto>        default: auto (sheet holding the required
                                   headers; pivot-cache sheets are ignored)
  --project-id / --dataset-id / --bq-location
  --transform-sql-gcs=<gs://...>   optional. Path to the transformation SQL in
                                   GCS; omit to use the copy embedded in this
                                   script (so a single uploaded .py file works).
  --measure=value|numerator|denominator   default: value (duplicate audit only)
  --main-metrics-sheet=<name>      optional, e.g. "Pivot Table 2". When set, its
                                   single column is read as the list of MAIN
                                   metrics and the audit flags is_main_metric.
  --dry-run=true|false             default: false (print everything, write nothing)

  Incremental processing (audit_table is the checkpoint, governs BOTH phases):
  --full-refresh=true|false        default: false. When false (the normal daily
                                   case) only new files, files whose Drive
                                   last-modified time changed, and files that
                                   failed before are processed; unchanged files
                                   are skipped end to end. Set true to force every
                                   file to be processed again.
  --reprocess-modified=true|false  default: true (re-process changed files)

  PHASE 1 (audit_table + metadata_table):
  --write-audit-metadata=true|false  default: true. NOTE: audit_table IS the
                                   checkpoint, so leaving this true is what makes
                                   incremental runs work.
  --audit-sheets=all|<index>|<name>  default: all (which sheets to profile)
  --audit-header=true|false          default: true (first non-empty row = header)
  --verbose=true|false             default: false (per-column schema + sample rows)

----------------------------------------------------------------------------
INCREMENTAL / CHECKPOINT LOGIC
----------------------------------------------------------------------------
Each run reads audit_table for the (file_id, last_modified) pairs that already
finished successfully (status SUCCESS or SKIPPED). A folder file is processed
only if that pair is absent - i.e. the file is new, was modified since, or
failed last time. A supported file is written to audit_table as SUCCESS only
after its pivot has actually landed in BigQuery; if the pivot fails it is
recorded FAILED and retried on the next run. So adding a few files per day
processes just those files, not the whole folder.
"""
import argparse
import hashlib
import io
import posixpath
import re
import unicodedata
import uuid
import zipfile
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

import requests
import google.auth
from google.auth import impersonated_credentials
from google.auth.transport.requests import Request, AuthorizedSession
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (StructType, StructField, StringType, LongType,
                               DoubleType, BooleanType, DateType, TimestampType)

# ===========================================================================
# Config
# ===========================================================================
DEFAULT_FOLDER_ID = "1O0nol9D0SXSDuBkNp3JBC6hyOxSqrEHx"

BQ_PROJECT_ID = "project-23dfabac-4beb-4269-a9d"
BQ_DATASET_ID = "project_1_srabani"
BQ_LOCATION = "us-central1"
DUP_DETAIL_TABLE = "signal_metric_duplicate_detail"
DUP_SUMMARY_TABLE = "signal_metric_duplicate_summary"
AUDIT_TABLE = "audit_table"
METADATA_TABLE = "metadata_table"
BQ_API = "https://bigquery.googleapis.com/bigquery/v2"

DRIVE_SCOPE = "https://www.googleapis.com/auth/drive.readonly"
DRIVE_API = "https://www.googleapis.com/drive/v3/files"
XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
XLSM_MIME = "application/vnd.ms-excel.sheet.macroEnabled.12"
GSHEET_MIME = "application/vnd.google-apps.spreadsheet"
FOLDER_MIME = "application/vnd.google-apps.folder"

# Columns the pivot needs; the sheet is auto-detected by looking for these.
KEY_COLUMNS = ["emp_cid", "ser_cid"]
METRIC_COLUMN = "metric"
REQUIRED_COLUMNS = KEY_COLUMNS + [METRIC_COLUMN, "value"]

# Descriptive columns carried through the pivot (cleaned names).
ATTRIBUTE_COLUMNS = [
    "clinician_name", "clinician_type", "login_service_area", "login_department",
    "specialty", "user_type", "reporting_period_start_date", "reporting_period_end_date",
]

# Non-metric columns of the cleaned table. Reserved so that no metric can ever
# clean into a name that collides with one of them.
RESERVED_OUTPUT_COLUMNS = [
    "metrics_reported", "source_row_count", "attr_conflicts",
    "run_id", "source_file_name", "source_file_id", "source_sheet_name",
    "load_timestamp_utc",
]

# Never let inference get these wrong, however the source stores them.
TYPE_OVERRIDES = {
    "emp_cid": "INT64",
    "ser_cid": "INT64",
    "metric_id": "INT64",
    "metric": "STRING",
    "clinician_name": "STRING",
    "numerator": "FLOAT64",
    "denominator": "FLOAT64",
    "value": "FLOAT64",
    "reporting_period_start_date": "DATE",
    "reporting_period_end_date": "DATE",
}

# Text that means "no value". Normalised to NULL on read, then filled in STEP 3.
NULL_LIKE = {"", "NULL", "N/A", "NA", "NONE", "NAN", "NIL", "(NULL)", "#N/A", "-", "--"}

# Epic Signal exports US dates. Order matters: first match wins.
DATE_FORMATS = ["%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y", "%d-%b-%Y", "%b %d, %Y", "%Y/%m/%d"]
TS_FORMATS = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%m/%d/%Y %H:%M:%S",
              "%m/%d/%Y %H:%M"]

STRING_FILL = "N/A"
INT_FILL = 0
TYPE_WIDTH = {"INT64": 0, "FLOAT64": 1, "BOOL": 2, "DATE": 3, "TIMESTAMP": 4, "STRING": 5}
BQ_RESERVED_PREFIXES = ("_table_", "_file_", "_partition", "_row_timestamp", "_colidentifier")

BAD_CATEGORIES = {"Cc", "Cf", "Cs", "Co", "Cn"}
ALLOWED_CONTROLS = {"\t", "\n", "\r"}
_XML_ESCAPE = re.compile(r"_x([0-9A-Fa-f]{4})_")
_DATE_BUILTIN = set(range(14, 23)) | set(range(27, 37)) | set(range(45, 48)) | set(range(50, 59))


# ===========================================================================
# XLSX parsing (standard library only, streaming)
# ===========================================================================
def _local(tag):
    return tag.rsplit("}", 1)[-1]


def _rel_id(elem):
    for k, v in elem.attrib.items():
        if k.endswith("}id"):
            return v
    return None


def _read_rels(zf, rels_path):
    rels = {}
    if rels_path not in zf.namelist():
        return rels
    for el in ET.fromstring(zf.read(rels_path)):
        if _local(el.tag) == "Relationship":
            rels[el.get("Id")] = (el.get("Target"), el.get("Type", ""))
    return rels


def _resolve(base_dir, target):
    if target.startswith("/"):
        return target.lstrip("/")
    return posixpath.normpath(posixpath.join(base_dir, target))


def _col_num(letters):
    n = 0
    for ch in letters:
        n = n * 26 + (ord(ch.upper()) - 64)
    return n


def _split_ref(ref):
    letters = "".join(ch for ch in ref if ch.isalpha())
    digits = "".join(ch for ch in ref if ch.isdigit())
    return (_col_num(letters) if letters else None), (int(digits) if digits else None)


def _unescape(s):
    return _XML_ESCAPE.sub(lambda m: chr(int(m.group(1), 16)), s)


def _rich_text(elem):
    parts = []
    for child in elem:
        name = _local(child.tag)
        if name == "t":
            parts.append(child.text or "")
        elif name == "r":
            for g in child:
                if _local(g.tag) == "t":
                    parts.append(g.text or "")
    return _unescape("".join(parts))


def _strip_bad_chars(s):
    """Drop control / invisible / replacement characters so BigQuery gets clean text."""
    if s.isascii():
        if not any((ord(c) < 32 and c not in ALLOWED_CONTROLS) or ord(c) == 127 for c in s):
            return s
        return "".join(c for c in s
                       if c in ALLOWED_CONTROLS or (32 <= ord(c) < 127))
    out = []
    for c in s:
        if c in ALLOWED_CONTROLS:
            out.append(c)
        elif c == "\ufffd" or unicodedata.category(c) in BAD_CATEGORIES:
            continue
        else:
            out.append(c)
    return "".join(out)


def _is_date_format(code):
    if not code:
        return False
    code = re.sub(r"\[[^\]]*\]", "", code)      # [Red], [$-409]
    code = re.sub(r'"[^"]*"', "", code)         # "literal"
    code = re.sub(r"\\.", "", code)             # escaped chars
    if re.search(r"general", code, re.I):
        return False
    return bool(re.search(r"[ymdhs]", code, re.I))


def _read_date_styles(zf, wb_dir, wb_rels):
    """Return the set of cellXfs indices whose number format is a date/time."""
    styles_path = None
    for target, rtype in wb_rels.values():
        if rtype.endswith("/styles"):
            styles_path = _resolve(wb_dir, target)
            break
    if not styles_path or styles_path not in zf.namelist():
        return set()

    root = ET.fromstring(zf.read(styles_path))
    custom = {}
    for el in root.iter():
        if _local(el.tag) == "numFmt":
            try:
                custom[int(el.get("numFmtId"))] = el.get("formatCode", "")
            except (TypeError, ValueError):
                pass

    date_styles = set()
    for el in root.iter():
        if _local(el.tag) != "cellXfs":
            continue
        for idx, xf in enumerate(el):
            if _local(xf.tag) != "xf":
                continue
            try:
                fmt_id = int(xf.get("numFmtId", "0"))
            except ValueError:
                continue
            if fmt_id in _DATE_BUILTIN or _is_date_format(custom.get(fmt_id)):
                date_styles.add(idx)
        break
    return date_styles


def _serial_to_datetime(serial, date1904):
    """Excel serial -> datetime (handles the 1900 leap-year bug and the 1904 system)."""
    try:
        serial = float(serial)
    except (TypeError, ValueError):
        return None
    if date1904:
        base = datetime(1904, 1, 1)
    else:
        base = datetime(1899, 12, 31) if serial < 60 else datetime(1899, 12, 30)
    try:
        return base + timedelta(days=serial)
    except OverflowError:
        return None


def open_workbook(content):
    """Return (zipfile, sheets, shared_strings, date_styles, date1904)."""
    try:
        zf = zipfile.ZipFile(io.BytesIO(content))
    except zipfile.BadZipFile:
        raise ValueError("Not a valid .xlsx file (password-protected, old .xls, "
                         "or another format renamed to .xlsx)")

    wb_path = "xl/workbook.xml"
    for target, rtype in _read_rels(zf, "_rels/.rels").values():
        if rtype.endswith("/officeDocument"):
            wb_path = _resolve("", target)
            break
    if wb_path not in zf.namelist():
        raise ValueError("Workbook part not found inside the file")
    wb_dir = posixpath.dirname(wb_path)
    wb_rels = _read_rels(zf, posixpath.join(wb_dir, "_rels",
                                            posixpath.basename(wb_path) + ".rels"))

    wb_root = ET.fromstring(zf.read(wb_path))
    date1904 = False
    sheets = []
    for el in wb_root.iter():
        name = _local(el.tag)
        if name == "workbookPr":
            date1904 = (el.get("date1904") or el.get("dateCompatibility") or "").lower() \
                in ("1", "true")
        elif name == "sheet":
            rid = _rel_id(el)
            if rid in wb_rels and wb_rels[rid][1].endswith("/worksheet"):
                sheets.append((el.get("name"), el.get("state", "visible"),
                               _resolve(wb_dir, wb_rels[rid][0])))

    shared = []
    for target, rtype in wb_rels.values():
        if rtype.endswith("/sharedStrings"):
            sst_path = _resolve(wb_dir, target)
            if sst_path in zf.namelist():
                with zf.open(sst_path) as fh:
                    for _, el in ET.iterparse(fh, events=("end",)):
                        if _local(el.tag) == "si":
                            shared.append(_rich_text(el))
                            el.clear()
            break

    return zf, sheets, shared, _read_date_styles(zf, wb_dir, wb_rels), date1904


def _cell_value(el, shared, date_styles, date1904):
    """Return the cell's Python value: str / int / float / bool / datetime / None."""
    t = el.get("t", "n")
    style = el.get("s")
    v_text, inline = None, None
    for child in el:
        name = _local(child.tag)
        if name == "v":
            v_text = child.text
        elif name == "is":
            inline = child

    if t == "inlineStr":
        value = _rich_text(inline) if inline is not None else None
    elif v_text is None:
        return None
    elif t == "s":
        try:
            value = shared[int(v_text)]
        except (ValueError, IndexError):
            return None
    elif t == "str":
        value = _unescape(v_text)
    elif t == "b":
        return v_text.strip() in ("1", "true", "TRUE")
    elif t == "e":
        return None                                  # #N/A, #DIV/0! -> NULL
    elif t == "d":                                   # ISO date (Strict Open XML)
        return _parse_text_datetime(v_text.strip())
    else:                                            # numeric
        raw = v_text.strip()
        try:
            style_idx = int(style) if style is not None else None
        except ValueError:
            style_idx = None
        if style_idx is not None and style_idx in date_styles:
            return _serial_to_datetime(raw, date1904)
        try:
            return int(raw) if re.fullmatch(r"[+-]?\d+", raw) else float(raw)
        except ValueError:
            return None

    if value is None:
        return None
    value = _strip_bad_chars(value).strip()
    return None if value.upper() in NULL_LIKE else value


def iter_sheet_rows(zf, sheet_path, shared, date_styles, date1904):
    """Yield {col_index: value} for every non-empty row, in sheet order."""
    row_cells, row_num, col_num = {}, 0, 0
    with zf.open(sheet_path) as fh:
        for event, el in ET.iterparse(fh, events=("start", "end")):
            name = _local(el.tag)
            if event == "start":
                if name == "row":
                    r = el.get("r")
                    row_num = int(r) if r else row_num + 1
                    col_num = 0
                    row_cells = {}
                continue
            if name == "c":
                ref = el.get("r")
                if ref:
                    c, _ = _split_ref(ref)
                    col_num = c if c else col_num + 1
                else:
                    col_num += 1
                value = _cell_value(el, shared, date_styles, date1904)
                if value is not None:
                    row_cells[col_num] = value
                el.clear()
            elif name == "row":
                if row_cells:
                    yield row_cells
                row_cells = {}
                el.clear()


def read_table(zf, sheet_path, shared, date_styles, date1904):
    """Return (raw_headers, rows) where rows are lists aligned to the headers."""
    rows_iter = iter_sheet_rows(zf, sheet_path, shared, date_styles, date1904)
    header_cells = next(rows_iter, None)
    if header_cells is None:
        return [], []
    width = max(header_cells)
    headers = [str(header_cells.get(c, "")).strip() for c in range(1, width + 1)]
    rows = [[r.get(c) for c in range(1, width + 1)] for r in rows_iter]
    return headers, rows


def read_single_column(zf, sheet_path, shared, date_styles, date1904):
    """Values of the first column of a sheet, header skipped (main-metric list)."""
    out = []
    for i, cells in enumerate(iter_sheet_rows(zf, sheet_path, shared, date_styles, date1904)):
        if i == 0:
            continue
        v = cells.get(1)
        if v is not None:
            out.append(str(v).strip())
    return out


# ===========================================================================
# Name cleaning + type inference
# ===========================================================================
def clean_name(name, used=None, fallback="col"):
    """BigQuery-safe column/table name: lower snake_case, deduplicated."""
    s = unicodedata.normalize("NFKD", str(name))
    s = s.encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^0-9a-zA-Z]+", "_", s).lower()
    s = re.sub(r"_+", "_", s)
    # Check reserved prefixes before stripping underscores: BigQuery rejects
    # _TABLE_*, _FILE_*, _PARTITION* and friends.
    reserved = s.startswith(BQ_RESERVED_PREFIXES)
    s = s.strip("_")
    if not s:
        s = fallback
    if s[0].isdigit() or reserved:
        s = "m_" + s
    s = s[:300]
    if used is not None:
        base, i = s, 2
        while s in used:
            suffix = "_%d" % i
            s = base[:300 - len(suffix)] + suffix
            i += 1
        used.add(s)
    return s


def _parse_text_datetime(text):
    for fmt in TS_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    return None


def _value_type(v):
    """BigQuery type implied by one Python value (None -> no evidence)."""
    if v is None:
        return None
    if isinstance(v, bool):
        return "BOOL"
    if isinstance(v, int):
        return "INT64"
    if isinstance(v, float):
        return "INT64" if v.is_integer() else "FLOAT64"
    if isinstance(v, datetime):
        return "DATE" if v.time() == datetime.min.time() else "TIMESTAMP"
    if isinstance(v, date):
        return "DATE"
    s = str(v).strip()
    if s.upper() in NULL_LIKE:
        return None
    if re.fullmatch(r"[+-]?\d{1,18}", s):
        return "INT64"
    if re.fullmatch(r"[+-]?(\d+\.\d*|\.\d+|\d+)([eE][+-]?\d+)?", s):
        return "FLOAT64"
    if s.lower() in ("true", "false", "yes", "no", "y", "n"):
        return "BOOL"
    parsed = _parse_text_datetime(s)
    if parsed is not None:
        return "DATE" if parsed.time() == datetime.min.time() else "TIMESTAMP"
    return "STRING"


def infer_types(headers, rows, sample_size=20000):
    """Widen a type per column across a sample of values. All-NULL -> STRING."""
    types = [None] * len(headers)
    step = max(1, len(rows) // sample_size) if rows else 1
    for r in rows[::step]:
        for i, v in enumerate(r):
            t = _value_type(v)
            if t is None:
                continue
            cur = types[i]
            if cur is None:
                types[i] = t
            elif cur != t:
                # INT64 + FLOAT64 -> FLOAT64; anything else incompatible -> STRING
                if {cur, t} == {"INT64", "FLOAT64"}:
                    types[i] = "FLOAT64"
                elif {cur, t} == {"DATE", "TIMESTAMP"}:
                    types[i] = "TIMESTAMP"
                else:
                    types[i] = max(cur, t, key=lambda x: TYPE_WIDTH[x])
                    if TYPE_WIDTH[types[i]] < TYPE_WIDTH["STRING"] and cur != t:
                        types[i] = "STRING"
    return ["STRING" if t is None else t for t in types]


def cast_value(v, bq_type):
    """Coerce a parsed cell into the inferred type, or NULL when impossible."""
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        if s.upper() in NULL_LIKE:
            return None
    else:
        s = None

    try:
        if bq_type == "STRING":
            if isinstance(v, datetime):
                return v.date().isoformat() if v.time() == datetime.min.time() \
                    else v.isoformat(sep=" ")
            if isinstance(v, bool):
                return "true" if v else "false"
            return str(v).strip()
        if bq_type == "INT64":
            if isinstance(v, bool):
                return int(v)
            if isinstance(v, (int, float)):
                return int(round(float(v)))
            return int(round(float(s.replace(",", ""))))
        if bq_type == "FLOAT64":
            if isinstance(v, bool):
                return float(v)
            if isinstance(v, (int, float)):
                return float(v)
            return float(s.replace(",", ""))
        if bq_type == "BOOL":
            if isinstance(v, bool):
                return v
            if isinstance(v, (int, float)):
                return bool(v)
            return s.lower() in ("true", "yes", "y", "1")
        if bq_type in ("DATE", "TIMESTAMP"):
            dt = v if isinstance(v, datetime) else (
                datetime(v.year, v.month, v.day) if isinstance(v, date)
                else _parse_text_datetime(s))
            if dt is None:
                return None
            if bq_type == "DATE":
                return dt.date()
            return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
    except (TypeError, ValueError, AttributeError):
        return None
    return None


_SPARK_TYPES = {"STRING": StringType, "INT64": LongType, "FLOAT64": DoubleType,
                "BOOL": BooleanType, "DATE": DateType, "TIMESTAMP": TimestampType}
_BQ_FROM_SPARK = {"string": "STRING", "bigint": "INT64", "int": "INT64",
                  "double": "FLOAT64", "float": "FLOAT64", "boolean": "BOOL",
                  "date": "DATE", "timestamp": "TIMESTAMP"}


def spark_schema(columns):
    """columns: [(name, bq_type, description)] -> Spark StructType."""
    return StructType([StructField(n, _SPARK_TYPES[t](), True) for n, t, _ in columns])


def bq_columns_from_df(df, descriptions=None):
    descriptions = descriptions or {}
    return [(f.name, _BQ_FROM_SPARK[f.dataType.simpleString()], descriptions.get(f.name, ""))
            for f in df.schema.fields]


# ===========================================================================
# Google Drive
# ===========================================================================
def get_sa_email():
    r = requests.get("http://metadata.google.internal/computeMetadata/v1/instance/"
                     "service-accounts/default/email",
                     headers={"Metadata-Flavor": "Google"}, timeout=10)
    r.raise_for_status()
    return r.text.strip()


def get_drive_token():
    source_creds, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"])
    target = impersonated_credentials.Credentials(
        source_credentials=source_creds, target_principal=get_sa_email(),
        target_scopes=[DRIVE_SCOPE], lifetime=3600)
    target.refresh(Request())
    return target.token


def list_folder_files(token, folder_id):
    headers = {"Authorization": "Bearer %s" % token}
    params = {
        "q": "'%s' in parents and trashed = false and mimeType != '%s'"
             % (folder_id, FOLDER_MIME),
        "fields": "nextPageToken, files(id, name, mimeType, size, modifiedTime)",
        "pageSize": 1000, "supportsAllDrives": "true", "includeItemsFromAllDrives": "true",
    }
    files = []
    while True:
        r = requests.get(DRIVE_API, headers=headers, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        files.extend(data.get("files", []))
        if not data.get("nextPageToken"):
            break
        params["pageToken"] = data["nextPageToken"]
    return files


def get_file(token, file_id):
    r = requests.get("%s/%s" % (DRIVE_API, file_id),
                     headers={"Authorization": "Bearer %s" % token},
                     params={"fields": "id, name, mimeType, size, modifiedTime",
                             "supportsAllDrives": "true"}, timeout=60)
    r.raise_for_status()
    return r.json()


def is_supported(f):
    return (f["mimeType"] in (XLSX_MIME, XLSM_MIME, GSHEET_MIME)
            or f["name"].lower().endswith((".xlsx", ".xlsm")))


def download_file(token, f):
    headers = {"Authorization": "Bearer %s" % token}
    if f["mimeType"] == GSHEET_MIME:
        url, params = "%s/%s/export" % (DRIVE_API, f["id"]), {"mimeType": XLSX_MIME}
    else:
        url, params = "%s/%s" % (DRIVE_API, f["id"]), {"alt": "media",
                                                       "supportsAllDrives": "true"}
    r = requests.get(url, headers=headers, params=params, timeout=900)
    r.raise_for_status()
    return r.content


# ===========================================================================
# BigQuery REST helpers
# ===========================================================================
def _bq_session():
    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    return AuthorizedSession(creds)


def ensure_dataset(session, project, dataset, location):
    r = session.get("%s/projects/%s/datasets/%s" % (BQ_API, project, dataset), timeout=60)
    if r.status_code == 200:
        return r.json().get("location")
    if r.status_code != 404:
        r.raise_for_status()
    body = {"datasetReference": {"projectId": project, "datasetId": dataset},
            "location": location,
            "description": "Epic Signal cleaned metric data and duplicate-metric audit"}
    r = session.post("%s/projects/%s/datasets" % (BQ_API, project), json=body, timeout=60)
    if r.status_code != 409:
        r.raise_for_status()
        print("Created BigQuery dataset %s.%s in %s" % (project, dataset, location))
    return location


def ensure_table(session, project, dataset, table, columns, description,
                 clustering=None, partition_field=None):
    """Create the table with the exact schema, or PATCH in any missing columns.
    Returns True if the table was created now, False if it already existed."""
    fields = [{"name": n, "type": t, "mode": "NULLABLE",
               "description": (d or "")[:1024]} for n, t, d in columns]
    url = "%s/projects/%s/datasets/%s/tables/%s" % (BQ_API, project, dataset, table)

    r = session.get(url, timeout=60)
    if r.status_code == 200:
        existing = {f["name"] for f in r.json().get("schema", {}).get("fields", [])}
        missing = [f for f in fields if f["name"] not in existing]
        if missing:
            keep = r.json()["schema"]["fields"]
            p = session.patch(url, json={"schema": {"fields": keep + missing}}, timeout=60)
            p.raise_for_status()
            print("Added %d new column(s) to %s.%s.%s" % (len(missing), project, dataset, table))
        return False
    if r.status_code != 404:
        r.raise_for_status()

    body = {"tableReference": {"projectId": project, "datasetId": dataset, "tableId": table},
            "description": description, "schema": {"fields": fields}}
    if partition_field:
        body["timePartitioning"] = {"type": "DAY", "field": partition_field}
    if clustering:
        body["clustering"] = {"fields": clustering[:4]}
    r = session.post("%s/projects/%s/datasets/%s/tables" % (BQ_API, project, dataset),
                     json=body, timeout=60)
    if r.status_code != 409:
        r.raise_for_status()
        print("Created BigQuery table %s.%s.%s (%d columns)"
              % (project, dataset, table, len(fields)))
        return True
    return False


def run_query(session, project, location, sql):
    body = {"query": sql, "useLegacySql": False, "location": location,
            "timeoutMs": 60000, "maxResults": 10000}
    r = session.post("%s/projects/%s/queries" % (BQ_API, project), json=body, timeout=120)
    r.raise_for_status()
    data = r.json()
    job_id = data["jobReference"]["jobId"]
    location = data["jobReference"].get("location", location)
    url = "%s/projects/%s/queries/%s" % (BQ_API, project, job_id)
    while not data.get("jobComplete"):
        r = session.get(url, params={"location": location, "timeoutMs": 60000}, timeout=120)
        r.raise_for_status()
        data = r.json()
    rows = [[c.get("v") for c in row["f"]] for row in data.get("rows", [])]
    while data.get("pageToken"):                       # pull every result page
        r = session.get(url, params={"location": location, "pageToken": data["pageToken"],
                                     "maxResults": 10000}, timeout=120)
        r.raise_for_status()
        data = r.json()
        rows.extend([c.get("v") for c in row["f"]] for row in data.get("rows", []))
    return rows


def write_to_bigquery(df, project, dataset, table):
    (df.write.format("bigquery")
       .option("writeMethod", "direct")
       .option("parentProject", project)
       .option("table", "%s.%s.%s" % (project, dataset, table))
       .mode("append")
       .save())


# ===========================================================================
# STEP 0 - load one file into a typed long DataFrame
# ===========================================================================
def pick_sheet(sheets, selector, zf, shared, date_styles, date1904):
    """Return (sheet_name, sheet_path). 'auto' = the sheet holding REQUIRED_COLUMNS."""
    visible = [s for s in sheets if s[1] == "visible"] or sheets
    if selector and selector != "auto":
        if selector.isdigit():
            idx = int(selector)
            if idx >= len(sheets):
                raise ValueError("Sheet index %d out of range; sheets: %s"
                                 % (idx, [s[0] for s in sheets]))
            return sheets[idx][0], sheets[idx][2]
        match = [s for s in sheets if s[0] == selector]
        if not match:
            raise ValueError("Sheet '%s' not found; sheets: %s"
                             % (selector, [s[0] for s in sheets]))
        return match[0][0], match[0][2]

    for name, _state, path in visible:
        rows = iter_sheet_rows(zf, path, shared, date_styles, date1904)
        header_cells = next(rows, None)
        if not header_cells:
            continue
        cleaned = {clean_name(v) for v in header_cells.values()}
        if set(REQUIRED_COLUMNS).issubset(cleaned):
            return name, path
    raise ValueError("No sheet contains the required columns %s; sheets: %s"
                     % (REQUIRED_COLUMNS, [s[0] for s in sheets]))


def load_long_dataframe(spark, zf, sheet_path, shared, date_styles, date1904, verbose=False):
    """Parse the sheet, infer types, and return (long_df, schema_report)."""
    raw_headers, rows = read_table(zf, sheet_path, shared, date_styles, date1904)
    if not raw_headers:
        raise ValueError("Sheet is empty")

    used = set()
    names = [clean_name(h, used, fallback="col_%d" % (i + 1))
             for i, h in enumerate(raw_headers)]

    inferred = infer_types(names, rows)
    types = [TYPE_OVERRIDES.get(n, t) for n, t in zip(names, inferred)]

    missing = [c for c in REQUIRED_COLUMNS if c not in names]
    if missing:
        raise ValueError("Required column(s) %s missing. Found: %s" % (missing, names))

    report = [{"source_header": h, "column_name": n, "inferred_type": i,
               "final_type": t, "overridden": i != t}
              for h, n, i, t in zip(raw_headers, names, inferred, types)]
    if verbose:                                    # 550+ metric columns -> noisy
        for r in report:
            print("  %-30s -> %-32s %-9s%s"
                  % (r["source_header"], r["column_name"], r["final_type"],
                     "  (override, inferred %s)" % r["inferred_type"] if r["overridden"] else ""))

    columns = [(n, t, "Source column: %s" % h) for n, t, h in zip(names, types, raw_headers)]
    # Rows without a key or a metric cannot be pivoted; cast and drop them in one
    # pass so the driver never holds two full copies of the file at once.
    key_idx = [names.index(c) for c in KEY_COLUMNS + [METRIC_COLUMN]]
    clean = []
    for r in rows:
        casted = tuple(cast_value(v, t) for v, t in zip(r, types))
        if all(casted[i] is not None for i in key_idx):
            clean.append(casted)
    dropped = len(rows) - len(clean)
    if dropped:
        print("  Dropped %d row(s) with a NULL key or metric" % dropped)

    df = spark.createDataFrame(clean, spark_schema(columns))
    return df, report, len(rows), dropped


# ===========================================================================
# STEP 1 - duplicate-metric audit (before the pivot)
# ===========================================================================
def metric_column_mapping(long_df, spark):
    """metric -> BigQuery-safe column name, collisions resolved deterministically."""
    metrics = sorted(r[0] for r in long_df.select(METRIC_COLUMN).distinct().collect()
                     if r[0] is not None)
    used = set(KEY_COLUMNS + ATTRIBUTE_COLUMNS + RESERVED_OUTPUT_COLUMNS)
    mapping = {m: clean_name(m, used, fallback="metric") for m in metrics}
    map_df = spark.createDataFrame(
        [(m, c) for m, c in mapping.items()],
        StructType([StructField(METRIC_COLUMN, StringType(), True),
                    StructField("metric_column", StringType(), True)]))
    return mapping, map_df


def duplicate_audit(keyed_df, measure, main_metrics):
    """
    Per (emp_cid, ser_cid, metric_column): how often does it appear, and do the
    repeats agree on the measure? Returns the per-metric detail DataFrame.

    Duplicates are keyed on metric_column (not the raw label) because that is
    exactly what the pivot collapses - so the audit reports what avg() hides.
    """
    # Compare the measure as a rounded string so that NULLs and float noise do
    # not inflate the "values disagree" count.
    value_key = F.coalesce(F.round(F.col(measure).cast("double"), 9).cast("string"),
                           F.lit("__NULL__"))
    metric_ids = (F.collect_set(F.col("metric_id").cast("string"))
                  if "metric_id" in keyed_df.columns
                  else F.array(F.lit(None).cast("string")))

    per_key = (keyed_df
               .groupBy(*KEY_COLUMNS, "metric_column")
               .agg(F.count(F.lit(1)).alias("occurrences"),
                    F.countDistinct(value_key).alias("distinct_values"),
                    F.collect_set(F.col(METRIC_COLUMN)).alias("raw_metrics"),
                    metric_ids.alias("metric_ids"),
                    F.min(F.col(measure).cast("double")).alias("min_value"),
                    F.max(F.col(measure).cast("double")).alias("max_value"))
               .filter(F.col("occurrences") > 1))

    detail = (per_key
              .groupBy("metric_column")
              .agg(F.countDistinct("ser_cid").alias("duplicate_ser_cid_count"),
                   F.countDistinct(F.struct(*KEY_COLUMNS)).alias("duplicate_key_count"),
                   F.sum(F.col("occurrences") - F.lit(1)).alias("extra_row_count"),
                   F.max("occurrences").alias("max_occurrences_per_ser_cid"),
                   F.sum(F.when(F.col("distinct_values") > 1, 1).otherwise(0))
                    .alias("conflicting_ser_cid_count"),
                   F.max(F.col("max_value") - F.col("min_value"))
                    .alias("max_value_spread"),
                   F.array_join(F.array_sort(F.flatten(F.collect_set("raw_metrics"))),
                                " | ").alias("raw_metric_labels"),
                   F.array_join(F.array_sort(F.flatten(F.collect_set("metric_ids"))),
                                ",").alias("metric_ids"),
                   F.array_join(F.slice(F.array_sort(F.collect_set(
                       F.col("ser_cid").cast("string"))), 1, 20), ",")
                    .alias("sample_ser_cids"))
              .withColumn("resolution", F.lit("AVG")))

    if main_metrics:
        main_array = F.array(*[F.lit(m) for m in sorted(set(main_metrics))])
        detail = detail.withColumn("is_main_metric",
                                   F.array_contains(main_array, F.col("metric_column")))
    else:
        detail = detail.withColumn("is_main_metric", F.lit(True))
    return per_key, detail


# ===========================================================================
# STEP 2/3 - pivot + null handling
# ===========================================================================
def pivot_and_clean(keyed, mapping, measure, fill_metric_nulls):
    """
    STEP 2: one row per (emp_cid, ser_cid), one column per metric, AVG on repeats.
    STEP 3: STRING -> 'N/A', INT64 -> 0, metric FLOAT64 -> 0.0 (optional).
    """
    pivot_values = sorted(set(mapping.values()))

    # The explicit value list makes this a single-pass pivot with a deterministic
    # column order, instead of Spark running a distinct() of its own first.
    pivoted = (keyed.groupBy(*KEY_COLUMNS)
                    .pivot("metric_column", pivot_values)
                    .agg(F.avg(F.col(measure).cast("double"))))

    attr_cols = [c for c in ATTRIBUTE_COLUMNS if c in keyed.columns]
    agg = [F.countDistinct("metric_column").alias("metrics_reported"),
           F.count(F.lit(1)).alias("source_row_count")]
    for c in attr_cols:
        agg.append(F.min(F.col(c)).alias(c))          # deterministic, unlike first()
        agg.append(F.countDistinct(F.col(c)).alias("__nd_%s" % c))
    attrs = keyed.groupBy(*KEY_COLUMNS).agg(*agg)

    # concat_ws skips NULLs, so this lists only the attributes that actually vary.
    conflict = F.concat_ws(",", *[F.when(F.col("__nd_%s" % c) > 1, F.lit(c))
                                  for c in attr_cols]) if attr_cols else F.lit("")
    attrs = (attrs.withColumn("attr_conflicts",
                              F.when(F.length(conflict) > 0, conflict)
                               .otherwise(F.lit(None).cast("string")))
                  .drop(*["__nd_%s" % c for c in attr_cols]))

    out = attrs.join(pivoted, on=KEY_COLUMNS, how="inner")

    metric_cols = pivot_values
    string_cols = [f.name for f in out.schema.fields
                   if isinstance(f.dataType, StringType) and f.name != "attr_conflicts"]
    int_cols = [f.name for f in out.schema.fields if isinstance(f.dataType, LongType)]

    out = out.fillna(STRING_FILL, subset=string_cols)
    out = out.fillna(INT_FILL, subset=int_cols)
    if fill_metric_nulls:
        out = out.fillna(0.0, subset=metric_cols)

    ordered = KEY_COLUMNS + attr_cols + ["metrics_reported", "source_row_count",
                                         "attr_conflicts"]
    return out.select(*ordered, *metric_cols), metric_cols


# ===========================================================================
# Target table naming
# ===========================================================================
_MONTH_NAMES = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12, "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6, "jul": 7,
    "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}


def parse_file_period(file_name):
    """(year, month) taken from the FILE NAME, or (None, None) if not found.

    Handles the SignalDownload naming seen in Drive, e.g.:
      SignalDownload_26-01-January.xlsx          -> (2026, 1)
      SignalDownload_MonthOf_25-12-December-IP   -> (2025, 12)
      SignalDownload_MonthOf_26-05-May-ED.xlsx   -> (2026, 5)
    A 2-digit year is read as 20YY. The numeric YY-MM group wins; the written
    month name is only a fallback so a typo'd number can't silently mislabel.
    """
    stem = file_name.rsplit(".", 1)[0]
    m = re.search(r"(?<!\d)(\d{4}|\d{2})[-_](\d{1,2})(?!\d)", stem)
    if m:
        yy, mm = m.group(1), int(m.group(2))
        year = int(yy) + 2000 if len(yy) == 2 else int(yy)
        if 1 <= mm <= 12:
            return year, mm
    name_hit = re.search(r"(\d{4}|\d{2}).*?([A-Za-z]{3,9})", stem)
    if name_hit:
        mm = _MONTH_NAMES.get(name_hit.group(2).lower())
        if mm:
            yy = name_hit.group(1)
            year = int(yy) + 2000 if len(yy) == 2 else int(yy)
            return year, mm
    return None, None


def resolve_table_name(fmt, file_name):
    """Table name from the FILE NAME's month + year (not the file's contents).

    signal_data_{yyyy}_{mm} -> signal_data_2026_01 for SignalDownload_26-01-January.
    If the name has no recognisable month/year, fall back to the cleaned file name
    so the file still lands somewhere obvious instead of failing.
    """
    year, month = parse_file_period(file_name)
    if year is None:
        prefix = fmt.split("{")[0].strip("_") or "signal_data"
        return clean_name("%s_%s" % (prefix, file_name.rsplit(".", 1)[0]),
                          fallback="signal_data")
    name = fmt.format(yyyy="%04d" % year,
                      yy="%02d" % (year % 100),
                      mm="%02d" % month,
                      dd="01",
                      file=file_name.rsplit(".", 1)[0])
    return clean_name(name, fallback="signal_data")


# ===========================================================================
# Audit table schemas
# ===========================================================================
DUP_DETAIL_COLUMNS = [
    ("run_id", "STRING", "Unique ID of this run"),
    ("target_table", "STRING", "Monthly table these duplicates belong to"),
    ("source_file_name", "STRING", "Source file in Google Drive"),
    ("source_file_id", "STRING", "Google Drive file ID"),
    ("source_sheet_name", "STRING", "Worksheet the data was read from"),
    ("reporting_period_start_date", "DATE", "Reporting period start of the file"),
    ("metric_column", "STRING", "Cleaned column name the metric pivots into"),
    ("raw_metric_labels", "STRING", "Original metric label(s) mapping to this column"),
    ("metric_ids", "STRING", "Distinct Metric IDs seen for this metric"),
    ("is_main_metric", "BOOL", "Metric is in the main-metric list (TRUE if no list given)"),
    ("duplicate_ser_cid_count", "INT64", "How many ser_cids have this metric more than once"),
    ("duplicate_key_count", "INT64", "Duplicated (emp_cid, ser_cid) pairs"),
    ("extra_row_count", "INT64", "Rows beyond the first occurrence (rows collapsed by AVG)"),
    ("max_occurrences_per_ser_cid", "INT64", "Worst repeat count for a single ser_cid"),
    ("conflicting_ser_cid_count", "INT64",
     "ser_cids whose duplicate rows disagree on the measure (real conflict)"),
    ("max_value_spread", "FLOAT64", "Largest max-min gap within a duplicated group"),
    ("sample_ser_cids", "STRING", "Up to 20 affected ser_cids"),
    ("resolution", "STRING", "How duplicates were resolved in the cleaned table"),
    ("audit_timestamp_utc", "TIMESTAMP", "Run start time (partition column)"),
]

DUP_SUMMARY_COLUMNS = [
    ("run_id", "STRING", "Unique ID of this run"),
    ("target_table", "STRING", "Monthly table this summary describes"),
    ("source_file_name", "STRING", "Source file in Google Drive"),
    ("source_file_id", "STRING", "Google Drive file ID"),
    ("source_sheet_name", "STRING", "Worksheet the data was read from"),
    ("reporting_period_start_date", "DATE", "Reporting period start of the file"),
    ("reporting_period_end_date", "DATE", "Reporting period end of the file"),
    ("source_rows_read", "INT64", "Data rows read from the sheet"),
    ("rows_dropped_null_key", "INT64", "Rows dropped for a NULL key or metric"),
    ("total_ser_cids", "INT64", "Distinct ser_cids in the file"),
    ("total_metrics", "INT64", "Distinct metrics in the file"),
    ("ser_cids_with_duplicate_metrics", "INT64",
     "Distinct ser_cids having at least one duplicated metric"),
    ("pct_ser_cids_with_duplicates", "FLOAT64", "Share of ser_cids affected, %"),
    ("duplicated_metric_count", "INT64", "Distinct metrics duplicated anywhere in the file"),
    ("conflicting_metric_count", "INT64",
     "Duplicated metrics where repeats disagree on the measure"),
    ("total_extra_rows", "INT64", "Total rows collapsed by AVG"),
    ("duplicated_metrics_sample", "STRING", "First 10 duplicated metric columns"),
    ("rows_written", "INT64", "Rows written to the final processed table for this file"),
    ("metric_columns_written", "INT64", "Derived metric columns in the final table"),
    ("measure", "STRING", "Source column used as the pivot measure"),
    ("audit_timestamp_utc", "TIMESTAMP", "Run start time (partition column)"),
]


# ===========================================================================
# Final transformation: Gdrive -> pivot -> clean -> THESE transforms -> ONE table
#
# The source long data is NOT landed in BigQuery. Each file's long rows are
# transformed in Spark and appended to this single table. The SQL below is the
# Spark-SQL equivalent of the BigQuery transformation script: it standardises the
# CPT level-of-service metrics, pivots the handful of metrics the derived columns
# need (SUM of numerator), then computes the appointment / chart-closure / notes /
# after-hours / level-of-service-mix columns with the same null rules (numeric
# NULL -> 0, string NULL -> 'N/A', dates stay NULL).
# ===========================================================================
FINAL_TABLE = "BQ_Final_Processed_data"

# Read from the long data by the transformation; any missing in a given file are
# added as typed NULLs first so the SQL never fails on an absent column.
TRANSFORM_INPUT_COLUMNS = {
    "emp_cid": "bigint", "ser_cid": "bigint", "clinician_type": "string",
    # Both spellings are ensured present so the transform populates service_area /
    # department whether the source header is "Service Area" or "Login Service Area".
    "service_area": "string", "login_service_area": "string",
    "department": "string", "login_department": "string",
    "specialty": "string", "user_type": "string", "clinician_name": "string",
    "reporting_period_start_date": "date", "reporting_period_end_date": "date",
    "metric": "string", "numerator": "double",
}

# Provenance columns appended to every final row (lineage + idempotent reload).
FINAL_PROVENANCE_COLUMNS = [
    ("source_file_name", "STRING", "Source file in Google Drive"),
    ("source_file_id", "STRING", "Google Drive file ID"),
    ("source_sheet_name", "STRING", "Worksheet the data was read from"),
    ("run_id", "STRING", "Unique ID of the run that wrote the row"),
    ("load_timestamp_utc", "TIMESTAMP", "When the row was written"),
]

# {view} is replaced with the temp view over one file's long data.
TRANSFORM_SQL = """
WITH base AS (
  SELECT
    COALESCE(emp_cid, 0) AS emp_cid,
    COALESCE(ser_cid, 0) AS ser_cid,
    clinician_type,
    COALESCE(service_area, login_service_area) AS service_area,
    COALESCE(department,   login_department)   AS department,
    specialty, user_type, clinician_name,
    reporting_period_start_date, reporting_period_end_date,
    metric,
    COALESCE(CAST(numerator AS DOUBLE), 0.0) AS numerator
  FROM {view}
),
pivoted AS (
  SELECT
    emp_cid, ser_cid, clinician_type, service_area, department,
    specialty, user_type, clinician_name,
    reporting_period_start_date, reporting_period_end_date,
    SUM(CASE WHEN metric LIKE '%99201%' THEN numerator END) AS np1,
    SUM(CASE WHEN metric LIKE '%99202%' THEN numerator END) AS np2,
    SUM(CASE WHEN metric LIKE '%99203%' THEN numerator END) AS np3,
    SUM(CASE WHEN metric LIKE '%99204%' THEN numerator END) AS np4,
    SUM(CASE WHEN metric LIKE '%99205%' THEN numerator END) AS np5,
    SUM(CASE WHEN metric LIKE '%99211%' THEN numerator END) AS ep1,
    SUM(CASE WHEN metric LIKE '%99212%' THEN numerator END) AS ep2,
    SUM(CASE WHEN metric LIKE '%99213%' THEN numerator END) AS ep3,
    SUM(CASE WHEN metric LIKE '%99214%' THEN numerator END) AS ep4,
    SUM(CASE WHEN metric LIKE '%99215%' THEN numerator END) AS ep5,
    SUM(CASE WHEN metric = 'Appointments per Day'              THEN numerator END) AS appt_per_day,
    SUM(CASE WHEN metric = 'Appointments Closed - Same Day'    THEN numerator END) AS ac_same,
    SUM(CASE WHEN metric = 'Appointments Closed - 1 to 2 Days' THEN numerator END) AS ac_12,
    SUM(CASE WHEN metric = 'Appointments Closed - 3 to 4 Days' THEN numerator END) AS ac_34,
    SUM(CASE WHEN metric = 'Appointments Closed - 5 to 6 Days' THEN numerator END) AS ac_56,
    SUM(CASE WHEN metric = 'Appointments Closed - 7+ Days'     THEN numerator END) AS ac_7p,
    SUM(CASE WHEN metric = 'Days with Appointments'            THEN numerator END) AS days_appt,
    SUM(CASE WHEN metric = 'Time in Notes per Appointment'     THEN numerator END) AS notes_per_appt,
    SUM(CASE WHEN metric = 'Time in Notes per Day'             THEN numerator END) AS notes_per_day,
    SUM(CASE WHEN metric = 'Pajama Time'                       THEN numerator END) AS pajama,
    SUM(CASE WHEN metric = 'Time On Unscheduled Days'          THEN numerator END) AS unsched,
    SUM(CASE WHEN metric = 'Time Outside Scheduled Hours'      THEN numerator END) AS outside_sched
  FROM base
  GROUP BY emp_cid, ser_cid, clinician_type, service_area, department,
           specialty, user_type, clinician_name,
           reporting_period_start_date, reporting_period_end_date
),
parsed AS (
  SELECT
    *,
    COALESCE(CAST(FLOOR((DATEDIFF(reporting_period_end_date,
                                  reporting_period_start_date) + 1) / 7) AS BIGINT), 0)
      AS calc_weeks,
    TRUNC(reporting_period_end_date, 'MONTH') AS calc_month_year,
    (COALESCE(ep1,0.0)+COALESCE(ep2,0.0)+COALESCE(ep3,0.0)+COALESCE(ep4,0.0)+COALESCE(ep5,0.0))
      AS calc_sum_est,
    (COALESCE(np1,0.0)+COALESCE(np2,0.0)+COALESCE(np3,0.0)+COALESCE(np4,0.0)+COALESCE(np5,0.0))
      AS calc_sum_new
  FROM pivoted
)
SELECT
  emp_cid,
  ser_cid,
  COALESCE(NULLIF(TRIM(CAST(clinician_type AS STRING)), ''), 'N/A') AS clinician_type,
  COALESCE(NULLIF(TRIM(CAST(service_area   AS STRING)), ''), 'N/A') AS service_area,
  COALESCE(NULLIF(TRIM(CAST(department     AS STRING)), ''), 'N/A') AS department,
  COALESCE(NULLIF(TRIM(CAST(specialty      AS STRING)), ''), 'N/A') AS specialty,
  COALESCE(NULLIF(TRIM(CAST(user_type          AS STRING)), ''), 'N/A') AS user_type,
  COALESCE(NULLIF(TRIM(CAST(clinician_name     AS STRING)), ''), 'N/A') AS clinician_name,
  reporting_period_start_date,
  reporting_period_end_date,
  calc_weeks       AS number_of_weeks_in_reporting_period,
  calc_month_year  AS month_year,
  COALESCE(ROUND((appt_per_day / NULLIF(calc_weeks, 0)) * 4, 5), 0)  AS no_of_appointment,
  COALESCE(ROUND(ac_same / NULLIF(appt_per_day, 0), 5), 0)          AS Same_Day_Rate,
  COALESCE(ROUND((COALESCE(ac_same,0)+COALESCE(ac_12,0))
                 / NULLIF(appt_per_day,0), 5), 0)                   AS Rate_1_2_Days,
  COALESCE(ROUND((COALESCE(ac_same,0)+COALESCE(ac_12,0)+COALESCE(ac_34,0))
                 / NULLIF(appt_per_day,0), 5), 0)                   AS Rate_3_4_Days,
  COALESCE(ROUND((COALESCE(ac_same,0)+COALESCE(ac_12,0)+COALESCE(ac_34,0)+COALESCE(ac_56,0))
                 / NULLIF(appt_per_day,0), 5), 0)                   AS Rate_5_6_Days,
  COALESCE(ROUND(ac_7p / NULLIF(appt_per_day, 0), 5), 0)           AS Rate_7_Plus_Days,
  COALESCE(CAST(ROUND(days_appt) AS BIGINT), 0)                    AS Days_with_Appointments,
  COALESCE(ROUND(notes_per_appt / NULLIF(appt_per_day, 0), 5), 0)  AS Calculated_Time_in_Notes_per_Appointment,
  COALESCE(ROUND(notes_per_day  / NULLIF(days_appt, 0), 5), 0)     AS Calculated_Time_in_Notes_per_Day,
  COALESCE(ROUND((pajama        / NULLIF(calc_weeks,0)) * 4 / 60, 5), 0) AS New_Numerator_Pajama_Time_hrs,
  COALESCE(ROUND((unsched       / NULLIF(calc_weeks,0)) * 4 / 60, 5), 0) AS New_Numerator_Time_On_Unscheduled_Days_hrs,
  COALESCE(ROUND((outside_sched / NULLIF(calc_weeks,0)) * 4 / 60, 5), 0) AS New_Numerator_Time_Outside_Scheduled_Hours_hrs,
  COALESCE(calc_sum_est, 0.0)                                      AS Sum_of_Established_Patients,
  COALESCE(ROUND(ep1 / NULLIF(calc_sum_est,0), 5), 0.0)           AS Established_Level_1,
  COALESCE(ROUND(ep2 / NULLIF(calc_sum_est,0), 5), 0.0)           AS Established_Level_2,
  COALESCE(ROUND(ep3 / NULLIF(calc_sum_est,0), 5), 0.0)           AS Established_Level_3,
  COALESCE(ROUND(ep4 / NULLIF(calc_sum_est,0), 5), 0.0)           AS Established_Level_4,
  COALESCE(ROUND(ep5 / NULLIF(calc_sum_est,0), 5), 0.0)           AS Established_Level_5,
  COALESCE(calc_sum_new, 0.0)                                     AS Sum_of_New_Patients,
  COALESCE(ROUND(np1 / NULLIF(calc_sum_new,0), 5), 0.0)          AS New_Patient_Level_1,
  COALESCE(ROUND(np2 / NULLIF(calc_sum_new,0), 5), 0.0)          AS New_Patient_Level_2,
  COALESCE(ROUND(np3 / NULLIF(calc_sum_new,0), 5), 0.0)          AS New_Patient_Level_3,
  COALESCE(ROUND(np4 / NULLIF(calc_sum_new,0), 5), 0.0)          AS New_Patient_Level_4,
  COALESCE(ROUND(np5 / NULLIF(calc_sum_new,0), 5), 0.0)          AS New_Patient_Level_5
FROM parsed
"""

# Columns that are NOT derived metrics, used only to count metric columns.
_FINAL_NON_METRIC = {
    "emp_cid", "ser_cid", "clinician_type", "service_area", "department",
    "specialty", "user_type", "clinician_name", "reporting_period_start_date",
    "reporting_period_end_date", "number_of_weeks_in_reporting_period", "month_year",
}


# ===========================================================================
# Main
# ===========================================================================
def _shuffle_parts(spark):
    """A modest partition count for POC-sized files. Pulling everything through a
    single partition serialises the 554-column pivot; 200 (the Spark default)
    spawns 200 tiny tasks per shuffle and drowns a small cluster in scheduling.
    AQE will still coalesce further when a stage turns out small."""
    try:
        return int(spark.conf.get("spark.sql.shuffle.partitions"))
    except Exception:
        return 32


def _read_gcs_text(session, gcs_uri):
    """Read a text object from gs://bucket/path using the BigQuery session's creds
    (cloud-platform scope covers GCS read)."""
    import urllib.parse
    bucket, _, obj = gcs_uri[5:].partition("/")
    url = ("https://storage.googleapis.com/storage/v1/b/%s/o/%s?alt=media"
           % (bucket, urllib.parse.quote(obj, safe="")))
    r = session.get(url, timeout=60)
    r.raise_for_status()
    return r.text


def load_transform_sql(session, args):
    """The transformation SQL: embedded by default, or from GCS if provided."""
    if args.transform_sql_gcs:
        print("Loading transformation SQL from %s" % args.transform_sql_gcs)
        return _read_gcs_text(session, args.transform_sql_gcs)
    return TRANSFORM_SQL


def transform_to_final(spark, long_df, sql_text, f, sheet_name, run_id, run_ts):
    """Apply the transformation to one file's long data and stamp provenance.
    No BigQuery source table is involved - this runs entirely on the long rows."""
    src = long_df
    for name, dtype in TRANSFORM_INPUT_COLUMNS.items():
        if name not in src.columns:                # a file may lack an optional column
            src = src.withColumn(name, F.lit(None).cast(dtype))
    src.createOrReplaceTempView("signal_long")
    try:
        final_df = spark.sql(sql_text.replace("{view}", "signal_long"))
    finally:
        spark.catalog.dropTempView("signal_long")
    return (final_df
            .withColumn("source_file_name", F.lit(f["name"]))
            .withColumn("source_file_id", F.lit(f["id"]))
            .withColumn("source_sheet_name", F.lit(sheet_name))
            .withColumn("run_id", F.lit(run_id))
            .withColumn("load_timestamp_utc", F.lit(run_ts).cast("timestamp")))


def process_file(spark, bq, args, f, token, run_id, run_ts, location, dry_run,
                 transform_sql):
    verbose = args.verbose.lower() == "true"
    print("\n==================================================================")
    print("FILE: %s  (%s)" % (f["name"], f["id"]))
    print("==================================================================")

    content = download_file(token, f)
    zf, sheets, shared, date_styles, date1904 = open_workbook(content)
    if not sheets:
        raise ValueError("No worksheets found in workbook")
    sheet_name, sheet_path = pick_sheet(sheets, args.sheet, zf, shared, date_styles, date1904)
    print("Sheet: %s   (date-formatted styles: %d, 1904 system: %s)"
          % (sheet_name, len(date_styles), date1904))

    main_metrics = None
    if args.main_metrics_sheet:
        match = [s for s in sheets if s[0] == args.main_metrics_sheet]
        if match:
            main_metrics = [clean_name(m, fallback="metric")
                            for m in read_single_column(zf, match[0][2], shared,
                                                        date_styles, date1904)]
            print("Main-metric list: %d metrics from sheet '%s'"
                  % (len(main_metrics), args.main_metrics_sheet))
        else:
            print("WARNING: main-metrics sheet '%s' not found; treating all metrics as main"
                  % args.main_metrics_sheet)

    print("\n-- STEP 0: schema inference ---------------------------------")
    long_df, _report, rows_read, rows_dropped = load_long_dataframe(
        spark, zf, sheet_path, shared, date_styles, date1904, verbose=verbose)
    # The DataFrame is born from one driver-side Python list (a single partition);
    # spread it across the cluster so the wide pivot and every groupBy below run
    # in parallel instead of on one core, then cache it for the many reads.
    long_df = long_df.repartition(_shuffle_parts(spark), "ser_cid").cache()
    print("Rows read: %d" % rows_read)

    # Reporting period comes from the file CONTENTS and is recorded in the audit
    # tables only - the table NAME is driven by the file name (resolved up front).
    # One min/max pass instead of two separate collect() jobs.
    start_col = "reporting_period_start_date"
    end_col = "reporting_period_end_date"
    period_start = period_end = None
    if start_col in long_df.columns or end_col in long_df.columns:
        aggs = []
        if start_col in long_df.columns:
            aggs.append(F.min(start_col).alias("ps"))
        if end_col in long_df.columns:
            aggs.append(F.max(end_col).alias("pe"))
        row = long_df.agg(*aggs).collect()[0].asDict()
        period_start, period_end = row.get("ps"), row.get("pe")

    target_table = FINAL_TABLE
    print("Reporting period (from file contents): %s -> %s   |   final table: %s"
          % (period_start, period_end, target_table))

    mapping, map_df = metric_column_mapping(long_df, spark)
    collisions = len(mapping) - len(set(mapping.values()))
    print("Metrics: %d distinct -> %d columns%s"
          % (len(mapping), len(set(mapping.values())),
             "  (%d name collision(s) suffixed)" % collisions if collisions else ""))

    print("\n-- STEP 1: duplicate-metric audit ---------------------------")
    keyed = long_df.join(F.broadcast(map_df), on=METRIC_COLUMN, how="inner").cache()
    per_key, detail = duplicate_audit(keyed, args.measure, main_metrics)
    per_key = per_key.cache()

    total_ser_cids = long_df.select("ser_cid").distinct().count()
    dup_ser_cids = per_key.select("ser_cid").distinct().count()
    detail = detail.cache()
    dup_metric_count = detail.count()

    detail_out = (detail
                  .withColumn("run_id", F.lit(run_id))
                  .withColumn("target_table", F.lit(target_table))
                  .withColumn("source_file_name", F.lit(f["name"]))
                  .withColumn("source_file_id", F.lit(f["id"]))
                  .withColumn("source_sheet_name", F.lit(sheet_name))
                  .withColumn("reporting_period_start_date",
                              F.lit(period_start).cast("date"))
                  .withColumn("audit_timestamp_utc", F.lit(run_ts).cast("timestamp"))
                  .select(*[n for n, _, _ in DUP_DETAIL_COLUMNS])
                  .orderBy(F.col("duplicate_ser_cid_count").desc(),
                           F.col("metric_column")))

    print("ser_cids with duplicated metrics: %d of %d (%.2f%%)"
          % (dup_ser_cids, total_ser_cids,
             100.0 * dup_ser_cids / total_ser_cids if total_ser_cids else 0.0))
    print("duplicated metrics: %d" % dup_metric_count)
    if dup_metric_count and verbose:
        detail_out.select("metric_column", "duplicate_ser_cid_count", "extra_row_count",
                          "max_occurrences_per_ser_cid", "conflicting_ser_cid_count",
                          "max_value_spread").show(50, truncate=False)

    print("\n-- STEP 2/3: transform (CPT standardize + SUM pivot + derived) ")
    final_df = transform_to_final(spark, long_df, transform_sql,
                                  f, sheet_name, run_id, run_ts).cache()
    rows_written = final_df.count()
    metric_cols = [c for c in final_df.columns
                   if c not in _FINAL_NON_METRIC
                   and c not in {n for n, _, _ in FINAL_PROVENANCE_COLUMNS}]
    print("Final rows for this file: %d  (%d derived metric columns)"
          % (rows_written, len(metric_cols)))
    if verbose:
        final_df.show(10, truncate=40)

    agg = detail.agg(
        F.coalesce(F.sum("extra_row_count"), F.lit(0)).alias("extra"),
        F.coalesce(F.sum(F.when(F.col("conflicting_ser_cid_count") > 0, 1).otherwise(0)),
                   F.lit(0)).alias("conflicting"),
        F.array_join(F.slice(F.array_sort(F.collect_set("metric_column")), 1, 10),
                     " | ").alias("sample")).collect()[0]

    summary_row = (run_id, target_table, f["name"], f["id"], sheet_name,
                   period_start, period_end, int(rows_read), int(rows_dropped),
                   int(total_ser_cids), int(len(mapping)), int(dup_ser_cids),
                   round(100.0 * dup_ser_cids / total_ser_cids, 4) if total_ser_cids else 0.0,
                   int(dup_metric_count), int(agg["conflicting"]), int(agg["extra"]),
                   agg["sample"], int(rows_written), int(len(metric_cols)),
                   "numerator (SUM)", run_ts)
    summary_df = spark.createDataFrame([summary_row], spark_schema(DUP_SUMMARY_COLUMNS))

    print("\n-- STEP 4: load -> single final table -----------------------")
    if dry_run:
        print("DRY RUN: nothing written to BigQuery")
        summary_df.show(truncate=False)
        final_df.unpersist()
        for d in (long_df, keyed, per_key, detail):
            d.unpersist()
        return

    # One shared final table, appended file by file. No monthly source tables.
    final_descriptions = dict((n, d) for n, _, d in FINAL_PROVENANCE_COLUMNS)
    created = ensure_table(bq, args.project_id, args.dataset_id, FINAL_TABLE,
                           bq_columns_from_df(final_df, final_descriptions),
                           "Final processed Signal metrics, one row per clinician-period, "
                           "appended from every source file",
                           clustering=["service_area", "specialty"],
                           partition_field="month_year")
    # Idempotent reload: if this file was loaded before (e.g. it was modified and
    # is being re-processed), drop its previous rows before appending the new ones.
    if not created:
        fid = f["id"].replace("'", "''")
        run_query(bq, args.project_id, location,
                  "DELETE FROM `%s.%s.%s` WHERE source_file_id = '%s'"
                  % (args.project_id, args.dataset_id, FINAL_TABLE, fid))
    write_to_bigquery(final_df, args.project_id, args.dataset_id, FINAL_TABLE)
    print("Appended %d rows to %s.%s.%s"
          % (rows_written, args.project_id, args.dataset_id, FINAL_TABLE))

    if dup_metric_count:
        write_to_bigquery(detail_out, args.project_id, args.dataset_id, DUP_DETAIL_TABLE)
    print("Wrote %d rows to %s.%s.%s"
          % (dup_metric_count, args.project_id, args.dataset_id, DUP_DETAIL_TABLE))
    write_to_bigquery(summary_df, args.project_id, args.dataset_id, DUP_SUMMARY_TABLE)
    print("Wrote 1 row to %s.%s.%s" % (args.project_id, args.dataset_id, DUP_SUMMARY_TABLE))

    for d in (long_df, keyed, per_key, detail, final_df):
        d.unpersist()


# ===========================================================================
# PHASE 1 helpers: pre-ingestion audit + per-sheet data-quality profile
# -> audit_table (one row per file/sheet) and metadata_table (one row per sheet)
# These parse the workbook independently of the pivot (STEP 0-4) above: the
# profiler reports raw-cell facts (nulls, duplicates, bad chars, formulas, ...),
# so it keeps its own cell reader rather than the type-inferring one.
# ===========================================================================
def _has_bad_char(s):
    if s.isascii():
        return any((ord(c) < 32 and c not in ALLOWED_CONTROLS) or ord(c) == 127 for c in s)
    for c in s:
        if c in ALLOWED_CONTROLS:
            continue
        if c == "\ufffd" or unicodedata.category(c) in BAD_CATEGORIES:
            return True
    return False


def _normalize_number(v):
    try:
        return repr(float(v))
    except ValueError:
        return v


def _profile_cell_value(el, shared):
    """Return (kind, value, has_formula, formula_without_value) for one cell.
    kind in {number, text, bool, error, date} or None for a null cell.
    Separate from _cell_value() above, which types cells for the pivot."""
    t = el.get("t", "n")
    v_text, inline, has_formula = None, None, False
    for child in el:
        name = _local(child.tag)
        if name == "v":
            v_text = child.text
        elif name == "is":
            inline = child
        elif name == "f":
            has_formula = True

    if t == "inlineStr":
        value = _rich_text(inline) if inline is not None else None
        kind = "text"
    elif v_text is None:
        return None, None, has_formula, has_formula
    elif t == "s":
        value, kind = shared[int(v_text)], "text"
    elif t == "str":
        value, kind = _unescape(v_text), "text"
    elif t == "b":
        value, kind = v_text.strip(), "bool"
    elif t == "e":
        value, kind = v_text.strip(), "error"
    elif t == "d":
        value, kind = v_text.strip(), "date"
    else:
        value, kind = _normalize_number(v_text.strip()), "number"

    if kind == "text":
        if value is None or (value.strip() == "" and not _has_bad_char(value)):
            return None, None, has_formula, False
    return kind, value, has_formula, False


# Profiler treats more text tokens as "NULL-like" than the pivot's NULL_LIKE set.
PROFILE_NULL_LIKE = {"NULL", "N/A", "NA", "NONE", "NAN", "NIL", "(NULL)", "#N/A"}


def profile_sheet(zf, sheet_path, shared, header=True):
    """Stream one worksheet and compute every data-quality check in a single pass."""
    s = defaultdict(int)
    min_col = max_col = None
    first_row = last_row = None
    header_row = None
    header_cells = {}
    col_nonnull = defaultdict(int)
    col_types = defaultdict(set)
    seen_rows = set()
    nonempty_data_rows = 0
    nonnull_data_cells = 0

    row_num = 0
    col_num = 0
    row_cells = []

    def finish_row():
        nonlocal header_row, nonempty_data_rows, nonnull_data_cells
        if not row_cells:
            return
        if header and header_row is None:
            header_row = row_num
            for c, kind, value in row_cells:
                header_cells[c] = value
            return
        nonempty_data_rows += 1
        key = []
        for c, kind, value in row_cells:
            nonnull_data_cells += 1
            col_nonnull[c] += 1
            if kind == "error":
                s["excel_error_count"] += 1
            else:
                col_types[c].add(kind)
            if kind == "text" and value.strip().upper() in PROFILE_NULL_LIKE:
                s["null_like_values_count"] += 1
            key.append((c, kind, value))
        digest = hashlib.blake2b(repr(key).encode("utf-8"), digest_size=16).digest()
        if digest in seen_rows:
            s["duplicates_count"] += 1
        else:
            seen_rows.add(digest)

    with zf.open(sheet_path) as fh:
        for event, el in ET.iterparse(fh, events=("start", "end")):
            name = _local(el.tag)
            if event == "start":
                if name == "row":
                    r = el.get("r")
                    row_num = int(r) if r else row_num + 1
                    col_num = 0
                    row_cells = []
                continue

            if name == "c":
                ref = el.get("r")
                if ref:
                    c, _ = _split_ref(ref)
                    col_num = c if c else col_num + 1
                else:
                    col_num += 1
                kind, value, has_formula, no_cache = _profile_cell_value(el, shared)
                if has_formula:
                    s["formula_cells_count"] += 1
                if no_cache:
                    s["formula_no_cached_value_count"] += 1
                if kind is not None:
                    if kind == "text":
                        if _has_bad_char(value):
                            s["bad_chars_count"] += 1
                        if value != value.strip():
                            s["leading_trailing_space_count"] += 1
                        if "\n" in value or "\r" in value or "\t" in value:
                            s["line_break_tab_count"] += 1
                    row_cells.append((col_num, kind, value))
                    min_col = col_num if min_col is None else min(min_col, col_num)
                    max_col = col_num if max_col is None else max(max_col, col_num)
                    first_row = row_num if first_row is None else first_row
                    last_row = row_num
                el.clear()
            elif name == "row":
                finish_row()
                row_cells = []
                el.clear()
            elif name == "mergeCell":
                s["merged_cells_count"] += 1

    if first_row is None:                      # sheet has no data at all
        row_count = col_count = 0
        cols = []
    else:
        data_start = header_row + 1 if header_row is not None else first_row
        row_count = max(last_row - data_start + 1, 0)
        cols = range(min_col, max_col + 1)
        col_count = len(cols)

    total_cells = row_count * col_count
    null_count = total_cells - nonnull_data_cells
    s["empty_rows_count"] = row_count - nonempty_data_rows
    s["empty_columns_count"] = sum(1 for c in cols if col_nonnull[c] == 0) if row_count else 0
    s["mixed_type_columns_count"] = sum(1 for c in cols if len(col_types[c]) > 1)

    if header and header_row is not None:
        s["blank_header_count"] = sum(1 for c in cols if c not in header_cells)
        names = defaultdict(int)
        for c in cols:
            if c in header_cells:
                names[str(header_cells[c]).strip().lower()] += 1
        s["duplicate_header_count"] = sum(n - 1 for n in names.values() if n > 1)

    s["row_count"] = row_count
    s["column_count"] = col_count
    s["null_count"] = null_count
    s["null_percentage"] = round(null_count * 100.0 / total_cells, 2) if total_cells else 0.0
    return s


METADATA_CHECKS = [
    "duplicates_count", "null_count", "null_percentage", "bad_chars_count",
    "leading_trailing_space_count", "line_break_tab_count",
    "null_like_values_count", "empty_rows_count", "empty_columns_count",
    "blank_header_count", "duplicate_header_count", "mixed_type_columns_count",
    "excel_error_count", "formula_cells_count", "formula_no_cached_value_count",
    "merged_cells_count",
]
INFORMATIONAL = {"null_count", "null_percentage", "formula_cells_count"}


def _parse_drive_time(value):
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def profile_file(f, token, folder_id, sheet_sel, header, run_ts, run_id):
    """Runs on an executor: download + profile one file -> (audit_rows, meta_rows)."""
    base = {
        "run_id": run_id,
        "file_name": f["name"],
        "source": "google_drive",
        "source_path": "drive/folders/%s/%s" % (folder_id, f["name"]),
        "file_id": f["id"],
        "mime_type": f["mimeType"],
        "last_modified_utc": _parse_drive_time(f.get("modifiedTime")),
        "audit_timestamp_utc": run_ts,
    }
    audit, meta = [], []

    if not is_supported(f):
        audit.append(dict(base, sheet_name=None, row_count=None, column_count=None,
                          file_size_bytes=int(f["size"]) if f.get("size") else None,
                          md5_checksum=None, status="SKIPPED",
                          error_message="Unsupported file type"))
        return audit, meta

    try:
        content = download_file(token, f)
        size, md5 = len(content), hashlib.md5(content).hexdigest()
        zf, sheets, shared, _ds, _d1904 = open_workbook(content)
        if not sheets:
            raise ValueError("No worksheets found in workbook")

        if sheet_sel == "all":
            selected = sheets
        elif sheet_sel.isdigit():
            idx = int(sheet_sel)
            if idx >= len(sheets):
                raise ValueError("Sheet index %d out of range; sheets: %s"
                                 % (idx, [s[0] for s in sheets]))
            selected = [sheets[idx]]
        else:
            selected = [s for s in sheets if s[0] == sheet_sel]
            if not selected:
                raise ValueError("Sheet '%s' not found; sheets: %s"
                                 % (sheet_sel, [s[0] for s in sheets]))

        for sheet_name, state, path in selected:
            pr = profile_sheet(zf, path, shared, header=header)
            audit.append(dict(base, sheet_name=sheet_name, row_count=pr["row_count"],
                              column_count=pr["column_count"], file_size_bytes=size,
                              md5_checksum=md5, status="SUCCESS", error_message=None))
            issues = [k for k in METADATA_CHECKS if k not in INFORMATIONAL and pr[k]]
            if pr["row_count"] == 0:
                issues.insert(0, "no_data_rows")
            row = {"run_id": run_id, "file_id": f["id"], "file_name": f["name"],
                   "sheet_name": sheet_name, "sheet_state": state,
                   "row_count": pr["row_count"], "column_count": pr["column_count"]}
            row.update({k: pr[k] for k in METADATA_CHECKS})
            row["dq_status"] = "WARN" if issues else "PASS"
            row["dq_issues"] = ", ".join(issues) if issues else None
            row["audit_timestamp_utc"] = run_ts
            meta.append(row)
    except Exception as e:
        audit.append(dict(base, sheet_name=None, row_count=None, column_count=None,
                          file_size_bytes=int(f["size"]) if f.get("size") else None,
                          md5_checksum=None, status="FAILED",
                          error_message=("%s: %s" % (type(e).__name__, e))[:500]))
    return audit, meta


# (column, BigQuery type, description) for the two profiling tables.
AUDIT_COLUMNS = [
    ("run_id", "STRING", "Unique ID of the extraction run"),
    ("file_name", "STRING", "File name in Google Drive"),
    ("sheet_name", "STRING", "Worksheet name (NULL if file skipped/failed)"),
    ("row_count", "INT64", "Data rows (excluding header)"),
    ("column_count", "INT64", "Columns in the data range"),
    ("source", "STRING", "Source system"),
    ("source_path", "STRING", "Location of the file at the source"),
    ("file_id", "STRING", "Google Drive file ID"),
    ("mime_type", "STRING", "Drive MIME type"),
    ("file_size_bytes", "INT64", "File size in bytes"),
    ("md5_checksum", "STRING", "MD5 of the downloaded file content"),
    ("last_modified_utc", "TIMESTAMP", "Last modified time in Google Drive"),
    ("status", "STRING", "SUCCESS / SKIPPED / FAILED"),
    ("error_message", "STRING", "Reason when SKIPPED or FAILED"),
    ("audit_timestamp_utc", "TIMESTAMP", "Run start time (partition column)"),
]

_CHECK_DESCRIPTIONS = {
    "duplicates_count": "Data rows identical to an earlier row (first occurrence not counted)",
    "null_count": "Empty or whitespace-only cells in the data range",
    "null_percentage": "null_count / (row_count * column_count) * 100",
    "bad_chars_count": "Cells with control, invisible, private-use or U+FFFD characters",
    "leading_trailing_space_count": "Text cells with leading/trailing whitespace",
    "line_break_tab_count": "Text cells containing line breaks or tabs",
    "null_like_values_count": "Text cells such as NULL, N/A, NONE, NAN",
    "empty_rows_count": "Fully empty rows inside the data range",
    "empty_columns_count": "Columns with no data values",
    "blank_header_count": "Columns with no header text",
    "duplicate_header_count": "Repeated header names (trimmed, case-insensitive)",
    "mixed_type_columns_count": "Columns mixing number/text/boolean/date values",
    "excel_error_count": "Cells holding Excel errors (#N/A, #DIV/0!, #REF! ...)",
    "formula_cells_count": "Cells containing a formula",
    "formula_no_cached_value_count": "Formula cells without a saved result",
    "merged_cells_count": "Merged cell ranges",
}

METADATA_COLUMNS = (
    [("run_id", "STRING", "Unique ID of the extraction run"),
     ("file_id", "STRING", "Google Drive file ID"),
     ("file_name", "STRING", "File name in Google Drive"),
     ("sheet_name", "STRING", "Worksheet name"),
     ("sheet_state", "STRING", "visible / hidden / veryHidden"),
     ("row_count", "INT64", "Data rows (excluding header)"),
     ("column_count", "INT64", "Columns in the data range")]
    + [(k, "FLOAT64" if k == "null_percentage" else "INT64", _CHECK_DESCRIPTIONS[k])
       for k in METADATA_CHECKS]
    + [("dq_status", "STRING", "PASS / WARN"),
       ("dq_issues", "STRING", "Checks that triggered WARN"),
       ("audit_timestamp_utc", "TIMESTAMP", "Run start time (partition column)")]
)

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def to_micros(drive_time):
    """Drive RFC3339 time -> integer microseconds since epoch (or None)."""
    dt = _parse_drive_time(drive_time)
    return None if dt is None else (dt - _EPOCH) // timedelta(microseconds=1)


def load_checkpoint(session, project, dataset, location):
    """{(file_id, modified_micros)} already profiled OK (SUCCESS or SKIPPED)."""
    sql = ("SELECT file_id, UNIX_MICROS(last_modified_utc) AS modified_us "
           "FROM `%s.%s.%s` "
           "WHERE status IN ('SUCCESS','SKIPPED') AND file_id IS NOT NULL "
           "GROUP BY file_id, modified_us" % (project, dataset, AUDIT_TABLE))
    return {(fid, int(us) if us is not None else None)
            for fid, us in run_query(session, project, location, sql)}


def select_files(files, checkpoint, reprocess_modified):
    """Split the folder listing into (to_process, counts) using the checkpoint."""
    done_ids = {fid for fid, _ in checkpoint}
    to_process = []
    counts = {"new": 0, "modified": 0, "unchanged": 0, "modified_ignored": 0}
    for f in files:
        key = (f["id"], to_micros(f.get("modifiedTime")))
        if key in checkpoint:
            counts["unchanged"] += 1
        elif f["id"] in done_ids:
            if reprocess_modified:
                counts["modified"] += 1
                to_process.append(f)
            else:
                counts["modified_ignored"] += 1
        else:
            counts["new"] += 1                 # includes previously FAILED files
            to_process.append(f)
    return to_process, counts


def load_table_assignments(session, project, dataset, location):
    """{file_id: target_table} from previous runs, so a re-processed file reuses
    its own monthly table and a brand-new file for an already-used month gets a
    fresh _copy name instead of silently overwriting another file's table."""
    sql = ("SELECT source_file_id, ANY_VALUE(target_table) AS t "
           "FROM `%s.%s.%s` "
           "WHERE source_file_id IS NOT NULL AND target_table IS NOT NULL "
           "GROUP BY source_file_id" % (project, dataset, DUP_SUMMARY_TABLE))
    try:
        return {fid: t for fid, t in run_query(session, project, location, sql) if fid}
    except Exception as e:                         # table may not exist on first run
        print("No prior table assignments (%s)" % type(e).__name__)
        return {}


def assign_target_tables(files_sorted, fmt, prior):
    """Give each file its monthly table name. Reuse the file's own prior name when
    the month is unchanged; otherwise pick base, then base_copy, base_copy2 ...
    avoiding any name already owned by a different file or taken earlier this run."""
    taken_by_others = set(prior.values())
    assigned = set()
    for f in files_sorted:
        base = resolve_table_name(fmt, f["name"])
        owned = prior.get(f["id"])
        if owned and (owned == base or owned.startswith(base + "_copy")):
            name = owned                            # same file, same month -> same table
        else:
            blocked = (taken_by_others - {owned}) | assigned
            name, k = base, 1
            while name in blocked:
                k += 1
                name = "%s_copy" % base if k == 2 else "%s_copy%d" % (base, k - 1)
        f["target_table"] = name
        assigned.add(name)
        tag = ""
        if owned and name == owned:
            tag = "  (re-using existing table)"
        elif name != base:
            tag = "  (month already used -> copy)"
        print("  %-45s -> %s%s" % (f["name"], name, tag))


def profile_files(spark, args, files, run_id, run_ts):
    """PHASE 1 compute: profile files in parallel on the cluster.
    Returns (audit_rows, meta_rows) as lists of dicts - nothing is written yet, so
    the audit status can be finalised only after the pivot has actually succeeded."""
    header = args.audit_header.lower() == "true"
    sheet_sel = args.audit_sheets
    folder_id = args.folder_id
    sc = spark.sparkContext

    def run_partition(part):
        token = get_drive_token()              # each executor mints its own token
        for f in part:
            yield profile_file(f, token, folder_id, sheet_sel, header, run_ts, run_id)

    results = (sc.parallelize(files, numSlices=min(len(files), 64))
                 .mapPartitions(run_partition)
                 .collect())
    audit_rows = [r for a, _ in results for r in a]
    meta_rows = [r for _, m in results for r in m]
    return audit_rows, meta_rows


def write_profiles(spark, bq, args, audit_rows, meta_rows, verbose, dry_run):
    """PHASE 1 write: push the profiling rows to metadata_table then audit_table.
    Audit is written LAST because it is the checkpoint - a file counts as done only
    once both its metadata and its (post-pivot) audit row are safely stored."""
    project, dataset = args.project_id, args.dataset_id
    if not audit_rows:
        print("Audit/metadata: no rows to write.")
        return

    audit_df = spark.createDataFrame(
        [tuple(r[n] for n, _, _ in AUDIT_COLUMNS) for r in audit_rows],
        spark_schema(AUDIT_COLUMNS)).orderBy("file_name", "sheet_name")
    meta_df = None
    if meta_rows:
        meta_df = spark.createDataFrame(
            [tuple(r[n] for n, _, _ in METADATA_COLUMNS) for r in meta_rows],
            spark_schema(METADATA_COLUMNS)).orderBy("file_name", "sheet_name")

    if verbose:
        print("\n-- audit_table (%d row(s)) --" % len(audit_rows))
        audit_df.show(n=min(len(audit_rows), 100) or 1, truncate=False)
        if meta_df is not None:
            print("\n-- metadata_table (%d row(s)) --" % len(meta_rows))
            meta_df.show(n=min(len(meta_rows), 100) or 1, truncate=False)

    if dry_run:
        print("DRY RUN: audit/metadata not written to BigQuery")
        return

    if meta_df is not None:
        write_to_bigquery(meta_df, project, dataset, METADATA_TABLE)
        print("Appended %d row(s) to %s.%s.%s"
              % (len(meta_rows), project, dataset, METADATA_TABLE))
    write_to_bigquery(audit_df, project, dataset, AUDIT_TABLE)
    print("Appended %d row(s) to %s.%s.%s"
          % (len(audit_rows), project, dataset, AUDIT_TABLE))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--folder-id", default=DEFAULT_FOLDER_ID)
    p.add_argument("--file-id", default=None)
    p.add_argument("--sheet", default="auto")
    p.add_argument("--project-id", default=BQ_PROJECT_ID)
    p.add_argument("--dataset-id", default=BQ_DATASET_ID)
    p.add_argument("--bq-location", default=BQ_LOCATION)
    p.add_argument("--measure", default="value",
                   choices=["value", "numerator", "denominator"],
                   help="Measure used only for the duplicate-metric audit")
    p.add_argument("--main-metrics-sheet", default=None)
    p.add_argument("--dry-run", default="false")
    # Final transformation
    p.add_argument("--transform-sql-gcs", default=None,
                   help="gs://bucket/path to the transformation SQL; omit to use "
                        "the version embedded in this script")
    # Incremental processing (governs BOTH phases via audit_table as the checkpoint)
    p.add_argument("--full-refresh", default="false",
                   help="Ignore the checkpoint and process every file again")
    p.add_argument("--reprocess-modified", default="true",
                   help="Re-process files whose Drive last-modified time changed")
    # PHASE 1 (audit_table + metadata_table) controls
    p.add_argument("--write-audit-metadata", default="true",
                   help="Profile files and populate audit_table + metadata_table")
    p.add_argument("--audit-sheets", default="all",
                   help="all | <index> | <name> : which sheets to profile")
    p.add_argument("--audit-header", default="true",
                   help="Treat the first non-empty row of each sheet as a header")
    p.add_argument("--verbose", default="false",
                   help="Print per-column schema and sample rows (noisy for 550+ metrics)")
    args = p.parse_args()
    dry_run = args.dry_run.lower() == "true"
    verbose = args.verbose.lower() == "true"
    full_refresh = args.full_refresh.lower() == "true"
    reprocess_modified = args.reprocess_modified.lower() == "true"
    write_audit = args.write_audit_metadata.lower() == "true"

    run_id = str(uuid.uuid4())
    run_ts = datetime.now(timezone.utc).replace(microsecond=0)
    print("run_id = %s" % run_id)

    spark = (SparkSession.builder.appName("signal-pivot-clean-bq")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.pivotMaxValues", "20000")
             .config("spark.sql.caseSensitive", "false")
             # POC files are small; 200 shuffle tasks per stage is pure overhead
             # across the many wide aggregations here. Keep it modest and let
             # adaptive execution coalesce/skew-split from there.
             .config("spark.sql.shuffle.partitions", "32")
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .config("spark.sql.adaptive.skewJoin.enabled", "true")
             .getOrCreate())

    bq = _bq_session()
    location = args.bq_location
    if not dry_run:
        location = ensure_dataset(bq, args.project_id, args.dataset_id, args.bq_location) \
            or args.bq_location
        # All shared tables exist before anything reads the checkpoint or writes.
        ensure_table(bq, args.project_id, args.dataset_id, AUDIT_TABLE, AUDIT_COLUMNS,
                     "One row per Drive file/sheet per extraction run",
                     clustering=["file_name"], partition_field="audit_timestamp_utc")
        ensure_table(bq, args.project_id, args.dataset_id, METADATA_TABLE, METADATA_COLUMNS,
                     "Data-quality profile per sheet per extraction run",
                     clustering=["file_name"], partition_field="audit_timestamp_utc")
        ensure_table(bq, args.project_id, args.dataset_id, DUP_DETAIL_TABLE,
                     DUP_DETAIL_COLUMNS,
                     "Which metrics are duplicated per source file, and how badly",
                     clustering=["source_file_id", "metric_column"],
                     partition_field="audit_timestamp_utc")
        ensure_table(bq, args.project_id, args.dataset_id, DUP_SUMMARY_TABLE,
                     DUP_SUMMARY_COLUMNS,
                     "How many ser_cids have duplicated metrics per source file",
                     clustering=["source_file_id"],
                     partition_field="audit_timestamp_utc")

    # The final table is created lazily by the first file (so its first write can
    # skip a needless DELETE). The transformation SQL is loaded once.
    transform_sql = load_transform_sql(bq, args)

    token = get_drive_token()
    if args.file_id:
        all_files = [get_file(token, args.file_id)]
    else:
        all_files = list_folder_files(token, args.folder_id)
    if not all_files:
        print("No files found (is the folder shared with %s?)" % get_sa_email())
        spark.stop()
        return

    # ----- INCREMENTAL CHECKPOINT (gates BOTH phases) -----------------------
    # audit_table records (file_id, last_modified, status). A file is re-processed
    # only when it is new, its Drive last-modified time changed, or it failed
    # before. Unchanged files are skipped end to end - never downloaded, profiled
    # or pivoted again - which is what keeps a daily "add a few files" run fast.
    if full_refresh or dry_run:
        to_process = all_files
        if full_refresh:
            print("FULL REFRESH: processing all %d file(s)" % len(all_files))
    else:
        checkpoint = load_checkpoint(bq, args.project_id, args.dataset_id, location)
        to_process, counts = select_files(all_files, checkpoint, reprocess_modified)
        print("Checkpoint: %d in folder | new/retry: %d | modified: %d | unchanged: %d%s"
              % (len(all_files), counts["new"], counts["modified"], counts["unchanged"],
                 (" | modified but ignored: %d" % counts["modified_ignored"])
                 if counts["modified_ignored"] else ""))
    if not to_process:
        print("Nothing new to process. No tables changed.")
        spark.stop()
        return

    # Supported spreadsheet files get transformed and appended to the final table.
    supported = sorted([f for f in to_process if is_supported(f)], key=lambda x: x["name"])

    # ----- PHASE 1: profile to_process (compute now, write after the pivot) --
    audit_rows, meta_rows = ([], [])
    if write_audit:
        print("\n########## PHASE 1: audit + data-quality metadata ##########")
        try:
            audit_rows, meta_rows = profile_files(spark, args, to_process, run_id, run_ts)
        except Exception as e:
            print("Profiling FAILED -> %s: %s" % (type(e).__name__, e))

    # ----- PHASE 2: transform + append to the single final table ------------
    print("\n########## PHASE 2: transform -> %s ##########" % FINAL_TABLE)
    print("%d supported file(s) to process" % len(supported))
    pivot_ok, pivot_err = set(), {}
    for f in supported:
        try:
            process_file(spark, bq, args, f, token, run_id, run_ts, location, dry_run,
                         transform_sql)
            pivot_ok.add(f["id"])
        except Exception as e:                                  # one bad file must not
            pivot_err[f["id"]] = "%s: %s" % (type(e).__name__, e)  # kill the run
            print("FAILED %s -> %s: %s" % (f["name"], type(e).__name__, e))

    # ----- Finalise the checkpoint ------------------------------------------
    # A supported file is only SUCCESS (and therefore skippable next run) once its
    # pivot actually landed in BigQuery; if the pivot failed, flip its audit rows
    # to FAILED so the next run retries it.
    for row in audit_rows:
        if row["status"] == "SUCCESS" and row["file_id"] in pivot_err:
            row["status"] = "FAILED"
            row["error_message"] = ("pivot failed: %s" % pivot_err[row["file_id"]])[:500]
    if write_audit:
        write_profiles(spark, bq, args, audit_rows, meta_rows, verbose, dry_run)

    if pivot_err:
        print("\n%d file(s) failed the pivot:" % len(pivot_err))
        for f in supported:
            if f["id"] in pivot_err:
                print("  %s -> %s" % (f["name"], pivot_err[f["id"]]))
    spark.stop()


if __name__ == "__main__":
    main()