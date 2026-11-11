"""
Signal pipeline orchestration (Cloud Composer / Airflow 2) - PRODUCTION GRADE.

    start_pipeline
       -> preflight_scan_drive          (list Drive + diff against audit checkpoint)
       -> send_prestart_email           (HTML notification with file inventory)
       -> trigger_dataproc_serverless_job
       -> send_completion_email         (HTML notification with full run report;
                                         runs even on batch failure)
       -> end_pipeline

Why it is now production-grade
------------------------------
* unique batch id per run+retry            -> retries never collide on ALREADY_EXISTS
* retries with delay + execution timeout   -> transient hiccups auto-recover
* deferrable mode                          -> releases the worker slot while Spark runs
* max_active_runs = 1                      -> two daily runs never touch the tables at once
* batch auto-expires via ttl               -> no lingering resources
* preflight scan + prestart email          -> you know what's about to happen BEFORE
                                              the batch starts (new vs modified vs
                                              unchanged vs retry)
* completion email on success AND failure  -> you always know the outcome; the email
                                              carries audit / DQ / dup / final-table
                                              counts pulled from BigQuery
* email sends never fail the data pipeline (unless email.fail_pipeline_on_email_error
  is set true in the yaml)

Everything that changes between environments or runs lives in
    dags/config/analytics_pipeline_config_srabani.yaml
so this DAG file is never edited to change a path, an argument, a schedule,
or an email recipient.
"""
from __future__ import annotations

import io
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import yaml
from airflow import DAG
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.utils.trigger_rule import TriggerRule

# Make sibling module email_utils_srabani.py importable regardless of which
# directory Composer invokes the parser from.
_DAG_DIR = os.path.dirname(os.path.abspath(__file__))
if _DAG_DIR not in sys.path:
    sys.path.insert(0, _DAG_DIR)
import email_utils_srabani as mail  # noqa: E402

# ---------------------------------------------------------------------------
# Load the config that sits beside this DAG file.
# ---------------------------------------------------------------------------
CONFIG_PATH = os.path.join(_DAG_DIR, "config", "analytics_pipeline_config_srabani.yaml")
with open(CONFIG_PATH) as _fh:
    CFG = yaml.safe_load(_fh) or {}


def cfg(path, default=None):
    """Read a dotted key from the config, returning default if absent/None."""
    node = CFG
    for key in path.split("."):
        if not isinstance(node, dict) or key not in node:
            return default
        node = node[key]
    return default if node is None else node


# ---------------------------------------------------------------------------
# Values from config
# ---------------------------------------------------------------------------
PROJECT_ID = cfg("gcp.project_id")
REGION = cfg("gcp.region", "us-central1")
SERVICE_ACCOUNT = cfg("gcp.service_account", "")
SUBNETWORK_URI = cfg("gcp.subnetwork_uri", "")
NETWORK_URI = cfg("gcp.network_uri", "")

MAIN_PY = cfg("code.main_python_file_uri")
TRANSFORM_SQL_URI = cfg("code.transform_sql_uri", "")
PYTHON_FILE_URIS = cfg("code.python_file_uris", []) or []
ARCHIVE_URIS = cfg("code.archive_uris", []) or []

RUNTIME_VERSION = str(cfg("dataproc.runtime_version", "2.2"))
BATCH_ID_PREFIX = cfg("dataproc.batch_id_prefix", "signal-pipeline")
TTL_SECONDS = int(cfg("dataproc.ttl_seconds", 7200))
DEFERRABLE = bool(cfg("dataproc.deferrable", True))
RUNTIME_PROPERTIES = {str(k): str(v)
                      for k, v in (cfg("dataproc.runtime_properties", {}) or {}).items()}

BQ_DATASET = cfg("job.bq_dataset")
BQ_LOCATION = cfg("job.bq_location", REGION)
DRIVE_FOLDER_ID = cfg("job.drive_folder_id")
FULL_REFRESH = bool(cfg("job.full_refresh", False))
EXTRA_ARGS = cfg("job.extra_args", []) or []

# Table names (overridable only if the PySpark job is also changed).
TBL_AUDIT       = cfg("job.tables.audit",       "audit_table")
TBL_METADATA    = cfg("job.tables.metadata",    "metadata_table")
TBL_DUP_DETAIL  = cfg("job.tables.dup_detail",  "signal_metric_duplicate_detail")
TBL_DUP_SUMMARY = cfg("job.tables.dup_summary", "signal_metric_duplicate_summary")
TBL_FINAL       = cfg("job.tables.final",       "BQ_Final_Processed_data")
BQ_TABLES = {"audit": TBL_AUDIT, "metadata": TBL_METADATA,
             "dup_detail": TBL_DUP_DETAIL, "dup_summary": TBL_DUP_SUMMARY,
             "final": TBL_FINAL}

SCHEDULE = cfg("airflow.schedule", "0 6 * * *")
_START = str(cfg("airflow.start_date", "2026-01-01"))
START_DATE = datetime.strptime(_START, "%Y-%m-%d").replace(tzinfo=timezone.utc)
CATCHUP = bool(cfg("airflow.catchup", False))
MAX_ACTIVE_RUNS = int(cfg("airflow.max_active_runs", 1))
RETRIES = int(cfg("airflow.retries", 2))
RETRY_DELAY = timedelta(minutes=int(cfg("airflow.retry_delay_minutes", 5)))
EXEC_TIMEOUT = timedelta(minutes=int(cfg("airflow.execution_timeout_minutes", 90)))
OWNER = cfg("airflow.owner", "data-eng")
TAGS = cfg("airflow.tags", ["signal"]) or ["signal"]

EMAIL_ENABLED = bool(cfg("email.enabled", False))
EMAIL_FAIL_PIPELINE = bool(cfg("email.fail_pipeline_on_email_error", False))
ENV_LABEL = cfg("email.environment", "unknown")
SUBJECT_PREFIX = cfg("email.subject_prefix", "[Signal Pipeline]")


def _email_smtp_cfg():
    return {
        "smtp_host":     cfg("email.smtp_host"),
        "smtp_port":     cfg("email.smtp_port", 587),
        "smtp_user":     cfg("email.smtp_user"),
        "smtp_password": cfg("email.smtp_password"),
        "from_address":  cfg("email.from_address"),
        "from_name":     cfg("email.from_name", ""),
    }


def _email_recipients():
    return (cfg("email.to_addresses", []) or [],
            cfg("email.cc_addresses", []) or [])


# Fail loudly at parse time if the essentials are missing (shows as an Airflow
# import error rather than a confusing runtime failure).
_missing = [name for name, val in {
    "gcp.project_id": PROJECT_ID,
    "code.main_python_file_uri": MAIN_PY,
    "job.bq_dataset": BQ_DATASET,
    "job.drive_folder_id": DRIVE_FOLDER_ID,
}.items() if not val]
if _missing:
    raise ValueError("pipeline_config.yaml is missing required keys: %s" % ", ".join(_missing))

if EMAIL_ENABLED:
    _email_missing = [k for k in ("smtp_host", "smtp_user", "smtp_password",
                                   "from_address") if not cfg("email.%s" % k)]
    if _email_missing:
        raise ValueError("email.enabled is true but missing keys: %s"
                         % ", ".join(_email_missing))
    if not cfg("email.to_addresses"):
        raise ValueError("email.enabled is true but email.to_addresses is empty")


# ===========================================================================
# PySpark job arg + batch builders (unchanged from previous version)
# ===========================================================================
def build_job_args():
    args = [
        "--project-id=%s" % PROJECT_ID,
        "--dataset-id=%s" % BQ_DATASET,
        "--bq-location=%s" % BQ_LOCATION,
        "--folder-id=%s" % DRIVE_FOLDER_ID,
    ]
    if TRANSFORM_SQL_URI:
        args.append("--transform-sql-gcs=%s" % TRANSFORM_SQL_URI)
    if FULL_REFRESH:
        args.append("--full-refresh=true")
    args.extend(EXTRA_ARGS)
    return args


def build_batch():
    pyspark_batch = {"main_python_file_uri": MAIN_PY, "args": build_job_args()}
    if PYTHON_FILE_URIS:
        pyspark_batch["python_file_uris"] = PYTHON_FILE_URIS
    if ARCHIVE_URIS:
        pyspark_batch["archive_uris"] = ARCHIVE_URIS

    runtime_config = {"version": RUNTIME_VERSION}
    if RUNTIME_PROPERTIES:
        runtime_config["properties"] = RUNTIME_PROPERTIES

    execution_config = {}
    if SERVICE_ACCOUNT:
        execution_config["service_account"] = SERVICE_ACCOUNT
    if SUBNETWORK_URI:
        execution_config["subnetwork_uri"] = SUBNETWORK_URI
    elif NETWORK_URI:
        execution_config["network_uri"] = NETWORK_URI
    if TTL_SECONDS:
        execution_config["ttl"] = {"seconds": TTL_SECONDS}

    batch = {
        "pyspark_batch": pyspark_batch,
        "runtime_config": runtime_config,
        "labels": {"pipeline": "signal", "orchestrator": "composer"},
    }
    if execution_config:
        batch["environment_config"] = {"execution_config": execution_config}
    return batch


# Unique per run AND per retry: <prefix>-<YYYYMMDD>-<try_number>. ds_nodash is
# digits only and try_number increments on retry, so no two attempts collide and
# the id stays a valid batch name (lowercase letters, digits, hyphens, <=63).
BATCH_ID = "%s-{{ ds_nodash }}-{{ ti.try_number }}" % BATCH_ID_PREFIX


# ===========================================================================
# Preflight: scan Drive folder + diff against BigQuery audit checkpoint
# ---------------------------------------------------------------------------
# We re-implement this logic in the DAG (rather than invoking the Spark job)
# so the pre-start email can show EXACTLY what the batch will do, before the
# batch starts. The logic mirrors list_folder_files() + load_checkpoint() +
# select_files() in gdrive_poc3_srabani.py.
# ===========================================================================
_DRIVE_API = "https://www.googleapis.com/drive/v3/files"
_DRIVE_SCOPE = "https://www.googleapis.com/auth/drive.readonly"
_FOLDER_MIME = "application/vnd.google-apps.folder"
_SUPPORTED_MIMES = {
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",  # .xlsx
    "application/vnd.ms-excel.sheet.macroEnabled.12",                     # .xlsm
    "application/vnd.google-apps.spreadsheet",                            # native Google Sheet
}


def _drive_token():
    """ADC token with Drive readonly scope (same pattern as the Spark job)."""
    import google.auth
    from google.auth.transport.requests import Request
    creds, _ = google.auth.default(scopes=[_DRIVE_SCOPE])
    if not creds.valid:
        creds.refresh(Request())
    return creds.token


def _list_drive_folder(folder_id):
    """Non-recursive listing of a Drive folder (same scope as the Spark job)."""
    import requests
    token = _drive_token()
    headers = {"Authorization": "Bearer %s" % token}
    files, page = [], None
    q = "'%s' in parents and trashed = false" % folder_id
    fields = ("nextPageToken,files(id,name,mimeType,size,"
              "modifiedTime,md5Checksum,parents)")
    while True:
        params = {"q": q, "fields": fields, "pageSize": 1000,
                  "supportsAllDrives": True,
                  "includeItemsFromAllDrives": True}
        if page:
            params["pageToken"] = page
        r = requests.get(_DRIVE_API, headers=headers, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        for f in data.get("files", []):
            if f.get("mimeType") != _FOLDER_MIME:
                files.append(f)
        page = data.get("nextPageToken")
        if not page:
            break
    return files


def _load_checkpoint():
    """{(file_id, modified_rfc3339_normalised)} already SUCCESS/SKIPPED."""
    hook = BigQueryHook(gcp_conn_id="google_cloud_default",
                        location=BQ_LOCATION, use_legacy_sql=False)
    sql = ("SELECT file_id, "
           "FORMAT_TIMESTAMP('%%Y-%%m-%%dT%%H:%%M:%%E*SZ', last_modified_utc) "
           "AS modified_iso "
           "FROM `%s.%s.%s` "
           "WHERE status IN ('SUCCESS','SKIPPED') AND file_id IS NOT NULL "
           "GROUP BY file_id, modified_iso" % (PROJECT_ID, BQ_DATASET, TBL_AUDIT))
    # Table may not exist on the very first run.
    try:
        rows = hook.get_records(sql)
    except Exception as e:
        print("Checkpoint query failed (treating as empty): %s: %s"
              % (type(e).__name__, e))
        return {}, {}
    checkpoint = {}
    latest_modified_by_id = {}
    for file_id, modified_iso in rows:
        if file_id is None:
            continue
        checkpoint[(file_id, modified_iso)] = True
        # Remember the newest previous modified time per file (for the email).
        prev = latest_modified_by_id.get(file_id)
        if prev is None or (modified_iso or "") > (prev or ""):
            latest_modified_by_id[file_id] = modified_iso
    return checkpoint, latest_modified_by_id


def _file_record(f, extra=None):
    """Compact dict used across XCom + email."""
    rec = {
        "id":       f.get("id"),
        "name":     f.get("name"),
        "mime":     f.get("mimeType"),
        "size":     int(f["size"]) if f.get("size") and f.get("size").isdigit()
                    else (int(f["size"]) if isinstance(f.get("size"), int) else None),
        "modified": f.get("modifiedTime"),
    }
    if extra:
        rec.update(extra)
    return rec


def preflight_scan(**context):
    """List the Drive folder, diff against the audit checkpoint, push XCom."""
    t0 = time.time()
    files = _list_drive_folder(DRIVE_FOLDER_ID)
    checkpoint, latest_mod_by_id = _load_checkpoint()

    new_files, modified_files, prev_failed_files = [], [], []
    unchanged_count = 0
    supported, unsupported = 0, 0

    # Also check if a given file previously FAILED (so it's being retried).
    # Our checkpoint only holds SUCCESS/SKIPPED. Any file seen in Drive
    # whose (id, modified) ISN'T in checkpoint but whose id WAS previously
    # seen was either modified OR previously failed.
    prev_seen_ids = set(latest_mod_by_id.keys())

    for f in files:
        if f.get("mimeType") in _SUPPORTED_MIMES:
            supported += 1
        else:
            unsupported += 1
            continue  # unsupported MIME types are not counted in processing lists

        fid = f.get("id")
        fmod = f.get("modifiedTime")
        key = (fid, fmod)

        if FULL_REFRESH:
            new_files.append(_file_record(f))
            continue

        if key in checkpoint:
            unchanged_count += 1
        elif fid in prev_seen_ids:
            # Same id, different modified time -> modified since last success,
            # OR id was recorded only as FAILED previously.
            prev = latest_mod_by_id.get(fid)
            if prev and prev != fmod:
                modified_files.append(_file_record(f, {"prev_modified": prev}))
            else:
                prev_failed_files.append(_file_record(f))
        else:
            new_files.append(_file_record(f))

    scan = {
        "total":             len(files),
        "supported":         supported,
        "unsupported":       unsupported,
        "new_files":         new_files,
        "modified_files":    modified_files,
        "unchanged_count":   unchanged_count,
        "prev_failed_files": prev_failed_files,
        "full_refresh":      FULL_REFRESH,
        "scan_duration_s":   round(time.time() - t0, 2),
        "scanned_at":        datetime.now(timezone.utc).isoformat(),
    }

    print("Preflight scan: total=%d supported=%d unsupported=%d "
          "new=%d modified=%d unchanged=%d prev_failed=%d full_refresh=%s"
          % (scan["total"], scan["supported"], scan["unsupported"],
             len(new_files), len(modified_files), unchanged_count,
             len(prev_failed_files), FULL_REFRESH))

    context["ti"].xcom_push(key="scan", value=scan)
    # Also stash the wall-clock start so completion task can resolve the run_id
    # by querying audit_table for rows at or after this instant.
    context["ti"].xcom_push(key="run_started_at",
                            value=datetime.now(timezone.utc).isoformat())
    return scan


# ===========================================================================
# Email tasks
# ===========================================================================
def _parse_iso(s):
    """Robust ISO-8601 -> aware datetime."""
    if not s:
        return None
    if isinstance(s, datetime):
        return s if s.tzinfo else s.replace(tzinfo=timezone.utc)
    # Python <=3.10 chokes on trailing 'Z'.
    return datetime.fromisoformat(str(s).replace("Z", "+00:00"))


def _send_safely(subject, html_body):
    """Send email; respect EMAIL_FAIL_PIPELINE for error handling."""
    if not EMAIL_ENABLED:
        print("email.enabled is false; skipping send.")
        raise AirflowSkipException("email disabled")
    to_list, cc_list = _email_recipients()
    try:
        mail.send_html_email(_email_smtp_cfg(), to_list, cc_list, subject, html_body)
        print("Sent to %s (cc=%s): %s" % (to_list, cc_list, subject))
    except Exception as e:
        msg = "Email send FAILED: %s: %s" % (type(e).__name__, e)
        print(msg)
        if EMAIL_FAIL_PIPELINE:
            raise AirflowException(msg)
        # Silent swallow - do not fail the pipeline because an email bounced.


def send_prestart_email(**context):
    ti = context["ti"]
    scan = ti.xcom_pull(task_ids="preflight_scan_drive", key="scan")
    if not scan:
        raise AirflowException("preflight_scan_drive produced no XCom 'scan'")

    # Normalise types for the renderer.
    scan_for_render = dict(scan)
    scan_for_render["scanned_at"] = _parse_iso(scan.get("scanned_at"))

    dag_run_id = context["dag_run"].run_id if context.get("dag_run") else "-"
    batch_id = BATCH_ID_PREFIX + "-" + context["ds_nodash"] + "-1"  # best-effort preview

    to_process = (len(scan["new_files"]) + len(scan["modified_files"])
                  + len(scan["prev_failed_files"]))
    verb = "starting"
    if to_process == 0 and not scan.get("full_refresh"):
        verb = "starting (nothing new to process)"

    subject = "%s %s - %d/%d file(s) to process - %s" % (
        SUBJECT_PREFIX, verb, to_process, scan["total"], context["ds"])

    html_body = mail.render_prestart_email(
        dag_id=context["dag"].dag_id,
        dag_run_id=dag_run_id,
        batch_id=batch_id,
        env_label=ENV_LABEL,
        folder_id=DRIVE_FOLDER_ID,
        scan=scan_for_render,
    )
    _send_safely(subject, html_body)


# ---- Post-run BQ queries --------------------------------------------------
def _bq_query(sql, params=None):
    """Return list[dict] of rows (parameterised)."""
    from google.cloud import bigquery
    hook = BigQueryHook(gcp_conn_id="google_cloud_default",
                        location=BQ_LOCATION, use_legacy_sql=False)
    client = hook.get_client(project_id=PROJECT_ID, location=BQ_LOCATION)
    job_cfg = bigquery.QueryJobConfig()
    if params:
        job_cfg.query_parameters = params
    job = client.query(sql, job_config=job_cfg)
    return [dict(r) for r in job.result()]


def _resolve_run_id(run_started_iso):
    """Find the run_id that this Airflow run produced, by scanning audit_table
    for the latest run that started at-or-after our preflight timestamp."""
    from google.cloud import bigquery
    sql = ("SELECT run_id, MIN(audit_timestamp_utc) AS run_start "
           "FROM `%s.%s.%s` "
           "WHERE audit_timestamp_utc >= @since "
           "GROUP BY run_id "
           "ORDER BY run_start DESC "
           "LIMIT 1" % (PROJECT_ID, BQ_DATASET, TBL_AUDIT))
    since = _parse_iso(run_started_iso)
    params = [bigquery.ScalarQueryParameter("since", "TIMESTAMP", since)]
    try:
        rows = _bq_query(sql, params)
    except Exception as e:
        print("resolve_run_id failed: %s: %s" % (type(e).__name__, e))
        return None
    return rows[0]["run_id"] if rows else None


def _collect_summary(run_id):
    """Pull everything the completion email needs for a given run_id."""
    from google.cloud import bigquery

    out = {}
    if not run_id:
        return out

    p_run = [bigquery.ScalarQueryParameter("run_id", "STRING", run_id)]

    def _try(sql, params=None, default=None):
        try:
            return _bq_query(sql, params)
        except Exception as e:
            print("query failed (%s): %s" % (type(e).__name__, e))
            return default if default is not None else []

    # --- audit rows ------------------------------------------------------
    audit_sql = ("SELECT file_name, sheet_name, row_count, column_count, "
                 "file_size_bytes, status, error_message, md5_checksum "
                 "FROM `%s.%s.%s` WHERE run_id = @run_id "
                 "ORDER BY file_name, sheet_name"
                 % (PROJECT_ID, BQ_DATASET, TBL_AUDIT))
    out["audit_rows"] = _try(audit_sql, p_run)

    audit_counts_sql = (
        "SELECT "
        "  COUNTIF(status='SUCCESS') AS success_cnt, "
        "  COUNTIF(status='FAILED')  AS failed_cnt,  "
        "  COUNTIF(status='SKIPPED') AS skipped_cnt, "
        "  COUNT(*)                  AS total_cnt,   "
        "  SUM(row_count)            AS total_rows,  "
        "  SUM(file_size_bytes)      AS total_bytes  "
        "FROM `%s.%s.%s` WHERE run_id = @run_id"
        % (PROJECT_ID, BQ_DATASET, TBL_AUDIT))
    ac = _try(audit_counts_sql, p_run)
    if ac:
        r = ac[0]
        out["audit_counts"] = {
            "SUCCESS": r.get("success_cnt") or 0,
            "FAILED":  r.get("failed_cnt") or 0,
            "SKIPPED": r.get("skipped_cnt") or 0,
            "total":   r.get("total_cnt") or 0,
            "total_rows":  r.get("total_rows") or 0,
            "total_bytes": r.get("total_bytes") or 0,
        }

    # --- metadata counts + WARN rows ------------------------------------
    md_counts_sql = (
        "SELECT "
        "  COUNTIF(dq_status='PASS') AS pass_cnt, "
        "  COUNTIF(dq_status='WARN') AS warn_cnt, "
        "  COUNT(*)                  AS total_cnt, "
        "  SUM(null_count)           AS total_nulls, "
        "  SUM(duplicates_count)     AS total_duplicates, "
        "  SUM(bad_chars_count)      AS total_bad_chars "
        "FROM `%s.%s.%s` WHERE run_id = @run_id"
        % (PROJECT_ID, BQ_DATASET, TBL_METADATA))
    mc = _try(md_counts_sql, p_run)
    if mc:
        r = mc[0]
        out["metadata_counts"] = {
            "PASS":  r.get("pass_cnt") or 0,
            "WARN":  r.get("warn_cnt") or 0,
            "total": r.get("total_cnt") or 0,
            "total_nulls":      r.get("total_nulls") or 0,
            "total_duplicates": r.get("total_duplicates") or 0,
            "total_bad_chars":  r.get("total_bad_chars") or 0,
        }

    md_warn_sql = (
        "SELECT file_name, sheet_name, dq_status, dq_issues, "
        "       null_count, duplicates_count, bad_chars_count "
        "FROM `%s.%s.%s` "
        "WHERE run_id = @run_id AND dq_status = 'WARN' "
        "ORDER BY file_name, sheet_name LIMIT 50"
        % (PROJECT_ID, BQ_DATASET, TBL_METADATA))
    out["metadata_warn_rows"] = _try(md_warn_sql, p_run)

    # --- dup summary -----------------------------------------------------
    dup_sum_sql = (
        "SELECT source_file_name, source_rows_read, "
        "       ser_cids_with_duplicate_metrics, pct_ser_cids_with_duplicates, "
        "       duplicated_metric_count, conflicting_metric_count, "
        "       total_extra_rows, rows_written, duplicated_metrics_sample "
        "FROM `%s.%s.%s` WHERE run_id = @run_id "
        "ORDER BY source_file_name" % (PROJECT_ID, BQ_DATASET, TBL_DUP_SUMMARY))
    out["dup_summary_rows"] = _try(dup_sum_sql, p_run)

    dup_tot_sql = (
        "SELECT "
        "  COUNT(*)                                AS files, "
        "  SUM(duplicated_metric_count)            AS duplicated_metrics, "
        "  SUM(conflicting_metric_count)           AS conflicting_metrics, "
        "  SUM(total_extra_rows)                   AS extra_rows, "
        "  SUM(ser_cids_with_duplicate_metrics)    AS ser_cids_with_dups, "
        "  SUM(rows_written)                       AS rows_written "
        "FROM `%s.%s.%s` WHERE run_id = @run_id"
        % (PROJECT_ID, BQ_DATASET, TBL_DUP_SUMMARY))
    dt = _try(dup_tot_sql, p_run)
    if dt:
        r = dt[0]
        out["dup_summary_totals"] = {
            "files": r.get("files") or 0,
            "duplicated_metrics":  r.get("duplicated_metrics") or 0,
            "conflicting_metrics": r.get("conflicting_metrics") or 0,
            "extra_rows":          r.get("extra_rows") or 0,
            "ser_cids_with_dups":  r.get("ser_cids_with_dups") or 0,
            "rows_written":        r.get("rows_written") or 0,
        }

    # --- top offending metrics ------------------------------------------
    dup_top_sql = (
        "SELECT source_file_name, metric_column, raw_metric_labels, "
        "       duplicate_ser_cid_count, extra_row_count, "
        "       max_occurrences_per_ser_cid, conflicting_ser_cid_count, "
        "       max_value_spread, is_main_metric "
        "FROM `%s.%s.%s` WHERE run_id = @run_id "
        "ORDER BY extra_row_count DESC, conflicting_ser_cid_count DESC "
        "LIMIT 10" % (PROJECT_ID, BQ_DATASET, TBL_DUP_DETAIL))
    out["dup_detail_top"] = _try(dup_top_sql, p_run)

    # --- final table stats (always current, not per-run) ----------------
    final_sql = (
        "SELECT COUNT(*) AS total_rows, "
        "       COUNT(DISTINCT ser_cid) AS distinct_ser_cids, "
        "       MIN(reporting_period_start_date) AS earliest_period, "
        "       MAX(reporting_period_end_date)   AS latest_period "
        "FROM `%s.%s.%s`" % (PROJECT_ID, BQ_DATASET, TBL_FINAL))
    fs = _try(final_sql)
    if fs:
        r = fs[0]
        out["final_table_stats"] = {
            "total_rows":        r.get("total_rows"),
            "distinct_ser_cids": r.get("distinct_ser_cids"),
            "earliest_period":   r.get("earliest_period"),
            "latest_period":     r.get("latest_period"),
        }

    return out


def send_completion_email(**context):
    ti = context["ti"]
    run_started_iso = ti.xcom_pull(task_ids="preflight_scan_drive",
                                   key="run_started_at")
    dag_run_id = context["dag_run"].run_id if context.get("dag_run") else "-"

    # Was the batch task successful?
    batch_state = ti.xcom_pull(task_ids="trigger_dataproc_serverless_job",
                               key="return_value")
    # The DataprocCreateBatchOperator pushes the batch resource dict as XCom on
    # success; on failure XCom is absent and we fall back to checking task state.
    from airflow.models.taskinstance import TaskInstance
    from airflow.utils.state import State
    batch_ti = context["dag_run"].get_task_instance("trigger_dataproc_serverless_job") \
        if context.get("dag_run") else None
    batch_state_str = (batch_ti.state if batch_ti else None) or "unknown"
    batch_succeeded = (batch_state_str == State.SUCCESS)

    batch_error = None
    if not batch_succeeded:
        batch_error = ("Dataproc batch task ended in state '%s'. "
                       "Open Composer Task Logs for the full traceback, "
                       "and Cloud Console -> Dataproc -> Batches for the Spark driver output."
                       % batch_state_str)

    batch_id_rendered = BATCH_ID_PREFIX + "-" + context["ds_nodash"] + "-" \
        + str(batch_ti.try_number if batch_ti else 1)

    # Try to correlate with the Spark job's run_id (via audit_table).
    run_id = _resolve_run_id(run_started_iso)
    summary = _collect_summary(run_id)

    started = _parse_iso(run_started_iso)
    finished = datetime.now(timezone.utc)

    # Decide severity for the subject line.
    audit_failed = (summary.get("audit_counts") or {}).get("FAILED", 0)
    if not batch_succeeded:
        verdict, icon = "FAILED", "[FAIL]"
    elif audit_failed:
        verdict, icon = "COMPLETED WITH FILE FAILURES", "[WARN]"
    else:
        verdict, icon = "SUCCESS", "[OK]"
    rows_written = (summary.get("dup_summary_totals") or {}).get("rows_written", 0)
    subject = "%s %s %s - %d row(s) loaded - %s" % (
        SUBJECT_PREFIX, icon, verdict, rows_written or 0, context["ds"])

    html_body = mail.render_completion_email(
        dag_id=context["dag"].dag_id,
        dag_run_id=dag_run_id,
        batch_id=batch_id_rendered,
        env_label=ENV_LABEL,
        batch_status=("SUCCESS" if batch_succeeded else batch_state_str),
        batch_error=batch_error,
        started_at=started,
        finished_at=finished,
        run_id=run_id,
        bq_project=PROJECT_ID,
        bq_dataset=BQ_DATASET,
        bq_tables=BQ_TABLES,
        summary=summary,
    )
    _send_safely(subject, html_body)


# ===========================================================================
# DAG
# ===========================================================================
DEFAULT_ARGS = {
    "owner": OWNER,
    "retries": RETRIES,
    "retry_delay": RETRY_DELAY,
    "execution_timeout": EXEC_TIMEOUT,
    "email_on_failure": False,  # we send our own emails
    "email_on_retry": False,
}

with DAG(
    dag_id="signal_gdrive_to_bq_pipeline_srabani",
    description="Gdrive -> pivot/clean/transform -> BQ_Final_Processed_data "
                "(Dataproc Serverless) + HTML notifications",
    schedule=SCHEDULE,
    start_date=START_DATE,
    catchup=CATCHUP,
    max_active_runs=MAX_ACTIVE_RUNS,
    default_args=DEFAULT_ARGS,
    tags=TAGS,
    doc_md=__doc__,
) as dag:

    start_pipeline = EmptyOperator(
        task_id="start_pipeline",
        doc_md="Entry point.",
    )

    preflight_scan_drive = PythonOperator(
        task_id="preflight_scan_drive",
        python_callable=preflight_scan,
        execution_timeout=timedelta(minutes=10),
        retries=1,
        retry_delay=timedelta(minutes=1),
        doc_md="List the Drive folder and diff against the BigQuery audit "
               "checkpoint to classify each file as new / modified / unchanged "
               "/ previously-failed. Pushes the inventory to XCom for the "
               "prestart email, and stashes a wall-clock timestamp used by the "
               "completion task to correlate the Spark run_id.",
    )

    send_prestart_email_task = PythonOperator(
        task_id="send_prestart_email",
        python_callable=send_prestart_email,
        execution_timeout=timedelta(minutes=5),
        retries=1,
        retry_delay=timedelta(minutes=1),
        doc_md="HTML email summarising the preflight scan. Non-blocking: "
               "unless email.fail_pipeline_on_email_error is true in the yaml, "
               "an SMTP failure here does NOT stop the batch.",
    )

    submit_kwargs = dict(
        task_id="trigger_dataproc_serverless_job",
        project_id=PROJECT_ID,
        region=REGION,
        batch=build_batch(),
        batch_id=BATCH_ID,
        doc_md="Submit the Dataproc Serverless PySpark batch and wait for it to finish.",
    )
    if DEFERRABLE:
        submit_kwargs["deferrable"] = True
        submit_kwargs["polling_interval_seconds"] = 20

    trigger_dataproc_serverless_job = DataprocCreateBatchOperator(**submit_kwargs)

    send_completion_email_task = PythonOperator(
        task_id="send_completion_email",
        python_callable=send_completion_email,
        execution_timeout=timedelta(minutes=10),
        retries=1,
        retry_delay=timedelta(minutes=1),
        trigger_rule=TriggerRule.ALL_DONE,   # fire on success AND failure
        doc_md="HTML email with the full post-run report: audit per file, "
               "data-quality metadata, duplicate summary + top offending "
               "metrics, and the current final-table stats. Runs even when "
               "the Dataproc task failed, so you are always notified.",
    )

    end_pipeline = EmptyOperator(
        task_id="end_pipeline",
        trigger_rule=TriggerRule.NONE_FAILED,
        doc_md="Reached when the batch (and its email) succeed. If the batch "
               "failed, end_pipeline is skipped - the completion email already "
               "went out.",
    )

    (start_pipeline
     >> preflight_scan_drive
     >> send_prestart_email_task
     >> trigger_dataproc_serverless_job
     >> send_completion_email_task
     >> end_pipeline)