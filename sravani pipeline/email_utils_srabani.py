"""
HTML email renderers + SMTP sender for the Signal pipeline.

Two renderers:
  render_prestart_email()   -> called before the Dataproc batch is submitted.
                               Shows what the batch WILL do (new / modified /
                               unchanged / previously-failed files).
  render_completion_email() -> called after the Dataproc batch finishes.
                               Shows what the batch DID do (audit, DQ, dup
                               summary, top offending metrics, final-table
                               stats). Also handles the batch-failed case.

One sender: send_html_email().

The HTML is email-client-safe:
  - All CSS inline (Gmail strips <style> in several contexts).
  - Layout uses <table>, not flex/grid.
  - Icons are emoji (render everywhere, no external image dependency).
  - Max width 680px for comfortable reading on mobile and desktop.

This module is NOT a DAG - it is a plain Python helper imported by the DAG.
Composer will try to parse it when scanning dags/, and will skip it after
finding no top-level DAG object.
"""
from __future__ import annotations

import html
import smtplib
import ssl
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr, make_msgid


# =====================================================================
# Design system
# =====================================================================
COLORS = {
    "primary":    "#4F46E5",   # indigo-600
    "primary_dk": "#3730A3",   # indigo-800 (for gradient end)
    "accent":     "#7C3AED",   # violet-600
    "success":    "#059669",   # emerald-600
    "success_bg": "#D1FAE5",   # emerald-100
    "warn":       "#D97706",   # amber-600
    "warn_bg":    "#FEF3C7",   # amber-100
    "danger":     "#DC2626",   # red-600
    "danger_bg":  "#FEE2E2",   # red-100
    "info":       "#2563EB",   # blue-600
    "info_bg":    "#DBEAFE",   # blue-100
    "neutral":    "#6B7280",   # gray-500
    "neutral_bg": "#F3F4F6",   # gray-100
    "page_bg":    "#F4F5F7",
    "card_bg":    "#FFFFFF",
    "border":     "#E5E7EB",
    "text":       "#111827",
    "text_muted": "#6B7280",
}

FONT_STACK = ("-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,"
              "Arial,sans-serif,'Apple Color Emoji','Segoe UI Emoji'")


# =====================================================================
# Primitive renderers
# =====================================================================
def _esc(s):
    """HTML-escape a value; None -> empty string."""
    return html.escape("" if s is None else str(s))


def _esc_with_breaks(s):
    """HTML-escape AND insert soft word-break opportunities (<wbr>) after
    underscores, hyphens, dots and forward slashes. Used for long tokens like
    filenames and MIME types so they wrap at natural seams instead of
    overflowing their table cell. Numbers (no separators) stay intact.
    """
    esc = _esc(s)
    for sep in ("_", "-", ".", "/"):
        esc = esc.replace(sep, sep + "<wbr>")
    return esc


def _fmt_int(n):
    """1234567 -> '1,234,567'. None -> '-'."""
    if n is None:
        return "-"
    try:
        return "{:,}".format(int(n))
    except (ValueError, TypeError):
        return _esc(n)


def _fmt_bytes(n):
    """Human-friendly byte size."""
    if n is None:
        return "-"
    try:
        n = int(n)
    except (ValueError, TypeError):
        return _esc(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return ("%.1f %s" % (n, unit)) if unit != "B" else ("%d %s" % (n, unit))
        n /= 1024.0


def _fmt_dt(dt):
    """Render a datetime/str as 'YYYY-MM-DD HH:MM:SS UTC'."""
    if dt is None:
        return "-"
    if isinstance(dt, str):
        return _esc(dt)
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    return _esc(dt)


def _fmt_duration(start, end):
    if not start or not end:
        return "-"
    try:
        secs = int((end - start).total_seconds())
    except Exception:
        return "-"
    if secs < 60:
        return "%ds" % secs
    if secs < 3600:
        return "%dm %ds" % (secs // 60, secs % 60)
    return "%dh %dm" % (secs // 3600, (secs % 3600) // 60)


def _badge(text, kind="neutral"):
    """Colored pill badge. kind: success|warn|danger|info|neutral."""
    fg = COLORS.get(kind, COLORS["neutral"])
    bg = COLORS.get("%s_bg" % kind, COLORS["neutral_bg"])
    return (
        '<span style="display:inline-block;padding:3px 10px;border-radius:999px;'
        'font-size:12px;font-weight:600;letter-spacing:.3px;'
        'background:%s;color:%s;border:1px solid %s;">%s</span>'
        % (bg, fg, fg, _esc(text))
    )


def _status_badge(status):
    """Map a job status to a colored badge."""
    s = (status or "").upper()
    if s in ("SUCCESS", "SUCCEEDED", "OK", "PASS"):
        return _badge(s or "SUCCESS", "success")
    if s in ("FAILED", "ERROR", "CANCELLED"):
        return _badge(s, "danger")
    if s in ("WARN", "WARNING", "SKIPPED"):
        return _badge(s, "warn")
    if s in ("RUNNING", "PENDING"):
        return _badge(s, "info")
    return _badge(s or "-", "neutral")


def _metric_tile(label, value, icon="", kind="primary", sublabel=""):
    """A single stat tile. Returns a <td> that goes inside _metric_row().

    The visual 'card' is a <div> inside the <td>, so the <td> itself can carry
    horizontal padding as a gutter without breaking the 25% width math.
    """
    color = COLORS.get(kind, COLORS["primary"])
    sublabel_html = ""
    if sublabel:
        sublabel_html = ('<div style="font-size:11px;color:' + COLORS["text_muted"]
                         + ';margin-top:4px;font-family:' + FONT_STACK + ';">'
                         + _esc(sublabel) + '</div>')
    inner = (
        '<div style="background:#FFFFFF;border:1px solid ' + COLORS["border"]
        + ';border-radius:10px;padding:14px 8px;text-align:center;">'
        '<div style="font-size:22px;line-height:1;margin-bottom:6px;">'
        + _esc(icon) + '</div>'
        '<div style="font-size:24px;font-weight:700;color:' + color
        + ';line-height:1.1;font-family:' + FONT_STACK + ';">'
        + _esc(value) + '</div>'
        '<div style="font-size:11px;color:' + COLORS["text_muted"]
        + ';margin-top:6px;text-transform:uppercase;'
        'letter-spacing:.5px;font-weight:600;font-family:' + FONT_STACK + ';">'
        + _esc(label) + '</div>'
        + sublabel_html +
        '</div>'
    )
    return ('<td valign="top" width="25%" style="padding:4px 4px;">'
            + inner + '</td>')


def _metric_row(tiles):
    """Render up to 4 tiles in one row. Pads with blank cells for alignment."""
    cells = list(tiles)
    while len(cells) % 4 != 0:
        cells.append('<td width="25%" style="padding:4px 4px;"></td>')
    rows_html = []
    for i in range(0, len(cells), 4):
        rows_html.append("<tr>" + "".join(cells[i:i + 4]) + "</tr>")
    return (
        '<table role="presentation" cellspacing="0" cellpadding="0" '
        'border="0" width="100%" style="border-collapse:collapse;'
        'table-layout:fixed;margin:4px 0;">'
        + "".join(rows_html) +
        '</table>'
    )


def _section_header(title, icon=""):
    return (
        '<h2 style="margin:28px 0 12px 0;font-size:16px;font-weight:700;'
        'color:%s;font-family:%s;letter-spacing:.2px;">'
        '<span style="font-size:18px;margin-right:8px;">%s</span>%s'
        '</h2>'
    ) % (COLORS["text"], FONT_STACK, _esc(icon), _esc(title))


def _data_table(headers, rows, col_aligns=None, col_widths=None, caption=None):
    """Render a nice striped data table.

    rows: list of lists of pre-rendered HTML strings (already escaped).
    col_aligns: optional list of 'left'|'right'|'center' per column.
    col_widths: optional list of integer percent values summing to ~100;
                when given, the table uses table-layout:fixed so long cell
                content wraps instead of expanding the table.
    """
    if col_aligns is None:
        col_aligns = ["left"] * len(headers)

    use_fixed = col_widths is not None

    # Build <colgroup> for fixed layout.
    colgroup = ""
    if use_fixed:
        colgroup = "<colgroup>" + "".join(
            '<col style="width:' + str(w) + '%;">' for w in col_widths
        ) + "</colgroup>"

    # Header: never break inside a word - headers are short labels.
    # Data:   break only at natural boundaries (whitespace, hyphens, underscores
    #         via the softer rule); numbers like "54,321" stay intact.
    th_wrap = "white-space:normal;word-break:normal;" if use_fixed else ""
    td_wrap = "overflow-wrap:break-word;word-break:normal;" if use_fixed else ""

    th_cells = []
    for i, h in enumerate(headers):
        th_cells.append(
            '<th align="' + col_aligns[i] + '" '
            'style="padding:9px 8px;font-size:12px;font-weight:700;color:'
            + COLORS["text"] + ';text-transform:uppercase;letter-spacing:.3px;'
            'border-bottom:2px solid ' + COLORS["primary"] + ';background:'
            + COLORS["neutral_bg"] + ';font-family:' + FONT_STACK + ';'
            + th_wrap + '">' + _esc(h) + '</th>'
        )
    th_html = "".join(th_cells)

    body_html_rows = []
    for ri, row in enumerate(rows):
        bg = "#FFFFFF" if ri % 2 == 0 else "#FAFAFB"
        tds = []
        for i, cell in enumerate(row):
            align = col_aligns[i] if i < len(col_aligns) else "left"
            tds.append(
                '<td align="' + align + '" '
                'style="padding:9px 8px;font-size:13px;color:' + COLORS["text"]
                + ';border-bottom:1px solid ' + COLORS["border"]
                + ';font-family:' + FONT_STACK + ';vertical-align:top;'
                + td_wrap + '">' + cell + '</td>'
            )
        body_html_rows.append(
            '<tr style="background:' + bg + ';">' + "".join(tds) + '</tr>'
        )

    caption_html = ""
    if caption:
        caption_html = (
            '<div style="font-size:12px;color:' + COLORS["text_muted"]
            + ';margin:6px 2px 0 2px;font-family:' + FONT_STACK + ';">'
            + _esc(caption) + '</div>'
        )

    layout = "table-layout:fixed;" if use_fixed else ""
    return (
        '<table role="presentation" cellspacing="0" cellpadding="0" border="0" '
        'width="100%" style="border-collapse:collapse;border:1px solid '
        + COLORS["border"] + ';border-radius:10px;overflow:hidden;' + layout + '">'
        + colgroup + '<thead><tr>' + th_html + '</tr></thead><tbody>'
        + "".join(body_html_rows) + '</tbody></table>' + caption_html
    )


def _empty_state(text, icon="🗂️"):
    return (
        '<div style="padding:28px;text-align:center;background:%s;'
        'border:1px dashed %s;border-radius:10px;color:%s;'
        'font-family:%s;font-size:13px;">'
        '<div style="font-size:30px;margin-bottom:8px;">%s</div>%s'
        '</div>'
    ) % (COLORS["neutral_bg"], COLORS["border"], COLORS["text_muted"],
         FONT_STACK, _esc(icon), _esc(text))


def _callout(text, kind="info", icon="ℹ️"):
    fg = COLORS.get(kind, COLORS["info"])
    bg = COLORS.get("%s_bg" % kind, COLORS["info_bg"])
    return (
        '<div style="padding:12px 14px;margin:14px 0;border-left:4px solid %s;'
        'background:%s;border-radius:6px;color:%s;font-family:%s;font-size:13px;'
        'line-height:1.5;">'
        '<span style="margin-right:6px;">%s</span>%s'
        '</div>'
    ) % (fg, bg, COLORS["text"], FONT_STACK, _esc(icon), _esc(text))


# =====================================================================
# Outer email shell (header + content + footer)
# =====================================================================
def _shell(subtitle, content_html, header_title, header_icon,
           header_kind, meta_rows, env_label):
    """
    header_kind: 'success'|'warn'|'danger'|'info' - tints the header strip.
    meta_rows: list of (label, value) tuples shown under the title.
    """
    header_from = COLORS.get(header_kind, COLORS["primary"])
    header_to = {
        "success": "#047857",
        "warn":    "#B45309",
        "danger":  "#991B1B",
        "info":    "#1D4ED8",
        "primary": COLORS["primary_dk"],
    }.get(header_kind, COLORS["primary_dk"])

    meta_html = ""
    if meta_rows:
        meta_html = (
            '<table role="presentation" cellpadding="0" cellspacing="0" border="0" '
            'style="margin-top:14px;">'
            + "".join(
                '<tr>'
                '<td style="padding:2px 14px 2px 0;font-size:12px;color:#E5E7FF;'
                'font-family:%s;text-transform:uppercase;letter-spacing:.6px;'
                'font-weight:600;">%s</td>'
                '<td style="padding:2px 0;font-size:13px;color:#FFFFFF;'
                'font-family:%s;font-weight:500;">%s</td>'
                '</tr>'
                % (FONT_STACK, _esc(label), FONT_STACK, _esc(value))
                for label, value in meta_rows
            )
            + '</table>'
        )

    return """<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{subject}</title>
</head>
<body style="margin:0;padding:0;background:{page_bg};">
<table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0"
       style="background:{page_bg};padding:24px 12px;">
<tr><td align="center">
  <table role="presentation" width="680" cellspacing="0" cellpadding="0" border="0"
         style="max-width:680px;width:100%;background:{card_bg};border-radius:14px;
                overflow:hidden;box-shadow:0 2px 8px rgba(17,24,39,0.06);">
    <!-- Header -->
    <tr>
      <td style="padding:28px 32px;background:linear-gradient(135deg,{hdr_from},{hdr_to});
                 background-color:{hdr_from};color:#FFFFFF;">
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
          <tr>
            <td style="font-size:36px;line-height:1;width:54px;vertical-align:top;">{hdr_icon}</td>
            <td style="vertical-align:top;">
              <div style="font-size:22px;font-weight:700;line-height:1.2;color:#FFFFFF;
                          font-family:{font};">{title}</div>
              <div style="font-size:14px;color:#E0E7FF;margin-top:4px;
                          font-family:{font};">{subtitle}</div>
              {meta}
            </td>
          </tr>
        </table>
      </td>
    </tr>
    <!-- Body -->
    <tr>
      <td style="padding:24px 32px 8px 32px;color:{text};font-family:{font};">
        {content}
      </td>
    </tr>
    <!-- Footer -->
    <tr>
      <td style="padding:20px 32px 28px 32px;border-top:1px solid {border};
                 background:#FAFBFC;">
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
          <tr>
            <td style="font-size:12px;color:{muted};font-family:{font};line-height:1.6;">
              <strong style="color:{text};">Suki Signal Pipeline</strong>
              &nbsp;&middot;&nbsp; Environment: <strong>{env}</strong>
              &nbsp;&middot;&nbsp; Automated notification &mdash; do not reply.
              <br/>
              <span style="color:{muted};">You are receiving this because your address
              is listed in <code>analytics_pipeline_config_srabani.yaml &gt; email.to_addresses</code>.</span>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</td></tr>
</table>
</body>
</html>""".format(
        subject=_esc(header_title),
        page_bg=COLORS["page_bg"],
        card_bg=COLORS["card_bg"],
        hdr_from=header_from,
        hdr_to=header_to,
        hdr_icon=_esc(header_icon),
        title=_esc(header_title),
        subtitle=_esc(subtitle),
        meta=meta_html,
        content=content_html,
        border=COLORS["border"],
        muted=COLORS["text_muted"],
        text=COLORS["text"],
        env=_esc(env_label),
        font=FONT_STACK,
    )


# =====================================================================
# PRESTART EMAIL
# =====================================================================
def render_prestart_email(dag_id, dag_run_id, batch_id, env_label,
                          folder_id, scan):
    """
    scan: dict from DAG preflight task:
      {
        "total": int, "supported": int, "unsupported": int,
        "new_files":        [{name, size, modified, mime}],
        "modified_files":   [{name, size, modified, mime, prev_modified}],
        "unchanged_count":  int,
        "prev_failed_files":[{name, size, modified, mime}],
        "full_refresh":     bool,
        "scan_duration_s":  float,
        "scanned_at":       datetime (UTC)
      }
    """
    new_n = len(scan["new_files"])
    mod_n = len(scan["modified_files"])
    retry_n = len(scan["prev_failed_files"])
    unch_n = scan["unchanged_count"]
    to_process = new_n + mod_n + retry_n
    total = scan["total"]

    # --- Hero tiles ------------------------------------------------------
    tiles = _metric_row([
        _metric_tile("Files in folder", _fmt_int(total), "📁", "primary"),
        _metric_tile("New",             _fmt_int(new_n), "🆕", "success"),
        _metric_tile("Modified",        _fmt_int(mod_n), "✏️", "info"),
        _metric_tile("Unchanged",       _fmt_int(unch_n), "💤", "neutral"),
    ])
    tiles2 = _metric_row([
        _metric_tile("To process this run", _fmt_int(to_process), "🚀",
                     "success" if to_process else "neutral"),
        _metric_tile("Retrying prior failures", _fmt_int(retry_n), "🔁",
                     "warn" if retry_n else "neutral"),
        _metric_tile("Supported",   _fmt_int(scan["supported"]), "✅", "success"),
        _metric_tile("Unsupported", _fmt_int(scan["unsupported"]), "🚫",
                     "warn" if scan["unsupported"] else "neutral"),
    ])

    # --- File details ----------------------------------------------------
    def _file_rows(files, include_prev=False):
        rows = []
        for f in files:
            mime_tail = (f.get("mime") or "").split("/")[-1][:32]
            row = [
                '<strong>' + _esc_with_breaks(f.get("name", "")) + '</strong>',
                _esc_with_breaks(mime_tail),
                _fmt_bytes(f.get("size")),
                _fmt_dt(f.get("modified")),
            ]
            if include_prev:
                row.append(_fmt_dt(f.get("prev_modified")))
            rows.append(row)
        return rows

    sections = []

    # Callout if full refresh is on
    if scan.get("full_refresh"):
        sections.append(_callout(
            "FULL REFRESH mode is enabled - the checkpoint is being ignored and "
            "every file in the folder will be reprocessed.",
            kind="warn", icon="⚠️"))

    # Callout if nothing to do
    if to_process == 0 and not scan.get("full_refresh"):
        sections.append(_callout(
            "No new or modified files detected. The Dataproc job will start but "
            "will exit immediately without touching any BigQuery tables.",
            kind="info", icon="💡"))

    # Column widths for the file tables (sums to 100).
    _w_files4 = [46, 18, 14, 22]           # file / type / size / modified
    _w_files5 = [36, 14, 10, 20, 20]       # file / type / size / modified / prev

    # New files
    sections.append(_section_header("New files", "🆕"))
    if new_n:
        sections.append(_data_table(
            headers=["File", "Type", "Size", "Last modified (UTC)"],
            rows=_file_rows(scan["new_files"]),
            col_aligns=["left", "left", "right", "left"],
            col_widths=_w_files4,
            caption="Never seen before - will be profiled, transformed, and appended."))
    else:
        sections.append(_empty_state("No new files since the last run.", "✨"))

    # Modified files
    sections.append(_section_header("Modified files", "✏️"))
    if mod_n:
        sections.append(_data_table(
            headers=["File", "Type", "Size",
                     "New modified (UTC)", "Previous modified (UTC)"],
            rows=_file_rows(scan["modified_files"], include_prev=True),
            col_aligns=["left", "left", "right", "left", "left"],
            col_widths=_w_files5,
            caption="Previously processed; the file's Drive last-modified time has changed. "
                    "Prior rows will be deleted from the final table and re-appended."))
    else:
        sections.append(_empty_state("No previously-processed files have been changed.", "💤"))

    # Retries
    if retry_n:
        sections.append(_section_header("Retrying prior failures", "🔁"))
        sections.append(_data_table(
            headers=["File", "Type", "Size", "Last modified (UTC)"],
            rows=_file_rows(scan["prev_failed_files"]),
            col_aligns=["left", "left", "right", "left"],
            col_widths=_w_files4,
            caption="These files failed on a previous run and will be retried."))

    content_html = tiles + tiles2 + "".join(sections)

    header_kind = "info" if to_process else "neutral"
    header_icon = "📂"
    header_title = "Pipeline starting"
    subtitle = ("About to submit the Dataproc Serverless batch. "
                "%d of %d file(s) will be processed." % (to_process, total))

    meta = [
        ("DAG",        dag_id),
        ("Run",        dag_run_id),
        ("Batch id",   batch_id),
        ("Drive folder", folder_id),
        ("Scanned at", _fmt_dt(scan["scanned_at"])),
    ]

    return _shell(
        subtitle=subtitle,
        content_html=content_html,
        header_title=header_title,
        header_icon=header_icon,
        header_kind=header_kind,
        meta_rows=meta,
        env_label=env_label,
    )


# =====================================================================
# COMPLETION EMAIL
# =====================================================================
def render_completion_email(dag_id, dag_run_id, batch_id, env_label,
                            batch_status, batch_error,
                            started_at, finished_at,
                            run_id, bq_project, bq_dataset, bq_tables,
                            summary):
    """
    summary: dict (may have missing keys if the batch failed early):
      {
        "audit_rows":        [{file_name, sheet_name, row_count, column_count,
                               file_size_bytes, status, error_message, md5_checksum}],
        "audit_counts":      {"SUCCESS": n, "FAILED": n, "SKIPPED": n, "total": n,
                              "total_rows": n, "total_bytes": n},
        "metadata_counts":   {"PASS": n, "WARN": n, "total": n,
                              "total_nulls": n, "total_duplicates": n,
                              "total_bad_chars": n},
        "metadata_warn_rows":[{file_name, sheet_name, dq_status, dq_issues,
                               null_count, duplicates_count, bad_chars_count}],
        "dup_summary_rows":  [{source_file_name, source_rows_read,
                               ser_cids_with_duplicate_metrics, pct_ser_cids_with_duplicates,
                               duplicated_metric_count, conflicting_metric_count,
                               total_extra_rows, rows_written, duplicated_metrics_sample}],
        "dup_summary_totals":{"files": n, "duplicated_metrics": n, "conflicting_metrics": n,
                              "extra_rows": n, "ser_cids_with_dups": n, "rows_written": n},
        "dup_detail_top":    [{source_file_name, metric_column, raw_metric_labels,
                               duplicate_ser_cid_count, extra_row_count,
                               max_occurrences_per_ser_cid, conflicting_ser_cid_count,
                               max_value_spread, is_main_metric}],
        "final_table_stats": {"total_rows": n, "distinct_ser_cids": n,
                              "earliest_period": date, "latest_period": date},
      }
      Any key may be absent / empty if that table was not written.
    """
    success = (batch_status or "").upper() in ("SUCCESS", "SUCCEEDED")
    had_file_failures = False
    if summary.get("audit_counts", {}).get("FAILED", 0) > 0:
        had_file_failures = True

    if not success:
        header_kind, header_icon = "danger", "❌"
        header_title = "Pipeline FAILED"
    elif had_file_failures:
        header_kind, header_icon = "warn", "⚠️"
        header_title = "Pipeline completed WITH FILE FAILURES"
    else:
        header_kind, header_icon = "success", "✅"
        header_title = "Pipeline completed successfully"

    duration = _fmt_duration(started_at, finished_at)
    audit_counts = summary.get("audit_counts") or {}
    md_counts    = summary.get("metadata_counts") or {}
    dup_totals   = summary.get("dup_summary_totals") or {}
    final_stats  = summary.get("final_table_stats") or {}

    # --- Hero tiles ------------------------------------------------------
    tiles_a = _metric_row([
        _metric_tile("Duration",       duration, "⏱️", "primary"),
        _metric_tile("Files touched",  _fmt_int(audit_counts.get("total")), "📁", "primary"),
        _metric_tile("Succeeded",      _fmt_int(audit_counts.get("SUCCESS")), "✅", "success"),
        _metric_tile("Failed",         _fmt_int(audit_counts.get("FAILED")), "❌",
                     "danger" if audit_counts.get("FAILED") else "neutral"),
    ])
    tiles_b = _metric_row([
        _metric_tile("Rows written (final table)",
                     _fmt_int(dup_totals.get("rows_written")), "📥", "primary"),
        _metric_tile("Data scanned", _fmt_bytes(audit_counts.get("total_bytes")),
                     "💾", "info"),
        _metric_tile("DQ status PASS", _fmt_int(md_counts.get("PASS")), "🟢",
                     "success" if md_counts.get("PASS") else "neutral"),
        _metric_tile("DQ status WARN", _fmt_int(md_counts.get("WARN")), "🟡",
                     "warn" if md_counts.get("WARN") else "neutral"),
    ])
    tiles_c = _metric_row([
        _metric_tile("Duplicated metrics", _fmt_int(dup_totals.get("duplicated_metrics")),
                     "🧬", "warn" if dup_totals.get("duplicated_metrics") else "neutral"),
        _metric_tile("Conflicting metrics", _fmt_int(dup_totals.get("conflicting_metrics")),
                     "⚡", "danger" if dup_totals.get("conflicting_metrics") else "neutral"),
        _metric_tile("ser_cids with dup",   _fmt_int(dup_totals.get("ser_cids_with_dups")),
                     "👥", "warn" if dup_totals.get("ser_cids_with_dups") else "neutral"),
        _metric_tile("Extra rows collapsed", _fmt_int(dup_totals.get("extra_rows")),
                     "🗜️", "neutral"),
    ])

    sections = [tiles_a, tiles_b, tiles_c]

    # ---------------- Batch-level error (if batch failed) ----------------
    if not success:
        sections.append(_section_header("Batch failure", "🛑"))
        sections.append(_callout(
            "The Dataproc Serverless batch did not complete successfully. "
            "No downstream BigQuery writes may have occurred, or they may be partial. "
            "Check the batch logs in the Google Cloud console for the full traceback.",
            kind="danger", icon="🛑"))
        if batch_error:
            sections.append(_callout(batch_error, kind="danger", icon="📝"))

    # ---------------- Audit (per-file outcome) ---------------------------
    sections.append(_section_header("Audit - per-file outcome", "🔍"))
    audit_rows = summary.get("audit_rows") or []
    if audit_rows:
        rows = []
        for r in audit_rows:
            err = r.get("error_message") or ""
            rows.append([
                '<strong>' + _esc_with_breaks(r.get("file_name")) + '</strong>',
                _esc(r.get("sheet_name") or "-"),
                _status_badge(r.get("status")),
                _fmt_int(r.get("row_count")),
                _fmt_int(r.get("column_count")),
                _fmt_bytes(r.get("file_size_bytes")),
                ('<span style="color:%s;">%s</span>'
                 % (COLORS["danger"], _esc(err[:160]))) if err else "",
            ])
        sections.append(_data_table(
            headers=["File", "Sheet", "Status", "Rows", "Cols", "Size", "Error"],
            rows=rows,
            col_aligns=["left", "left", "center", "right", "right", "right", "left"],
            col_widths=[25, 13, 13, 11, 7, 10, 21],
            caption="Source: `%s.%s.%s`" % (bq_project, bq_dataset, bq_tables["audit"])))
    else:
        sections.append(_empty_state(
            "No audit rows were written for this run.", "🫥"))

    # ---------------- Data-quality metadata ------------------------------
    sections.append(_section_header("Data-quality metadata", "🧪"))
    md_warn = summary.get("metadata_warn_rows") or []
    # Summary line
    sections.append(
        '<div style="font-size:13px;color:%s;font-family:%s;margin-bottom:10px;">'
        'Total nulls detected: <strong>%s</strong> &nbsp;&middot;&nbsp; '
        'Row-level duplicates: <strong>%s</strong> &nbsp;&middot;&nbsp; '
        'Bad chars: <strong>%s</strong>'
        '</div>'
        % (COLORS["text"], FONT_STACK,
           _fmt_int(md_counts.get("total_nulls")),
           _fmt_int(md_counts.get("total_duplicates")),
           _fmt_int(md_counts.get("total_bad_chars")))
    )
    if md_warn:
        rows = [[
            '<strong>' + _esc_with_breaks(r.get("file_name")) + '</strong>',
            _esc(r.get("sheet_name") or "-"),
            _status_badge(r.get("dq_status")),
            _fmt_int(r.get("null_count")),
            _fmt_int(r.get("duplicates_count")),
            _fmt_int(r.get("bad_chars_count")),
            _esc((r.get("dq_issues") or "")[:200]),
        ] for r in md_warn]
        sections.append(_data_table(
            headers=["File", "Sheet", "DQ", "Nulls", "Dup rows", "Bad chars", "Issues"],
            rows=rows,
            col_aligns=["left", "left", "left", "right", "right", "right", "left"],
            col_widths=[26, 13, 9, 9, 10, 10, 23],
            caption="Only sheets with DQ status = WARN are listed. "
                    "Source: `%s.%s.%s`" % (bq_project, bq_dataset, bq_tables["metadata"])))
    else:
        if md_counts.get("total"):
            sections.append(_empty_state(
                "All profiled sheets passed the data-quality checks.", "🟢"))
        else:
            sections.append(_empty_state(
                "No data-quality metadata was written for this run.", "🫥"))

    # ---------------- Duplicate summary (per file) -----------------------
    sections.append(_section_header("Duplicate summary - per file", "🧬"))
    dup_rows = summary.get("dup_summary_rows") or []
    if dup_rows:
        rows = []
        for r in dup_rows:
            pct = r.get("pct_ser_cids_with_duplicates")
            pct_str = "-" if pct is None else ("%.2f%%" % pct)
            rows.append([
                '<strong>' + _esc_with_breaks(r.get("source_file_name")) + '</strong>',
                _fmt_int(r.get("source_rows_read")),
                _fmt_int(r.get("ser_cids_with_duplicate_metrics")),
                pct_str,
                _fmt_int(r.get("duplicated_metric_count")),
                _fmt_int(r.get("conflicting_metric_count")),
                _fmt_int(r.get("total_extra_rows")),
                _fmt_int(r.get("rows_written")),
            ])
        sections.append(_data_table(
            headers=["File", "Src rows", "Dup ser_cids", "% ser_cids",
                     "Dup metrics", "Conflicts", "Extra rows", "To final"],
            rows=rows,
            col_aligns=["left"] + ["right"] * 7,
            col_widths=[26, 10, 11, 11, 11, 10, 10, 11],
            caption="Source: `%s.%s.%s`" % (bq_project, bq_dataset, bq_tables["dup_summary"])))
    else:
        sections.append(_empty_state(
            "No duplicate-summary rows were written this run.", "✨"))

    # ---------------- Top offending metrics ------------------------------
    dup_detail_top = summary.get("dup_detail_top") or []
    if dup_detail_top:
        sections.append(_section_header("Top offending metrics (by extra rows)", "⚡"))
        rows = []
        for r in dup_detail_top:
            spread = r.get("max_value_spread")
            spread_str = "-" if spread is None else ("%.4f" % spread)
            main = _badge("MAIN", "info") if r.get("is_main_metric") else ""
            rows.append([
                '<strong>' + _esc_with_breaks(r.get("source_file_name")) + '</strong>',
                _esc_with_breaks(r.get("metric_column")) + ' ' + main,
                _esc((r.get("raw_metric_labels") or "")[:60]),
                _fmt_int(r.get("duplicate_ser_cid_count")),
                _fmt_int(r.get("extra_row_count")),
                _fmt_int(r.get("max_occurrences_per_ser_cid")),
                _fmt_int(r.get("conflicting_ser_cid_count")),
                spread_str,
            ])
        sections.append(_data_table(
            headers=["File", "Metric column", "Raw label",
                     "Dup ser_cids", "Extra rows", "Max repeats",
                     "Conflicts", "Max spread"],
            rows=rows,
            col_aligns=["left", "left", "left", "right", "right", "right", "right", "right"],
            col_widths=[22, 14, 16, 10, 9, 10, 9, 10],
            caption="Top 10 by extra row count. Source: `%s.%s.%s`"
                    % (bq_project, bq_dataset, bq_tables["dup_detail"])))

    # ---------------- Final table stats ----------------------------------
    sections.append(_section_header("Final table", "📊"))
    if final_stats and final_stats.get("total_rows") is not None:
        headers = ["Total rows (cumulative)", "Distinct ser_cids",
                   "Earliest period", "Latest period"]
        row = [
            _fmt_int(final_stats.get("total_rows")),
            _fmt_int(final_stats.get("distinct_ser_cids")),
            _fmt_dt(final_stats.get("earliest_period"))
                if final_stats.get("earliest_period") else "-",
            _fmt_dt(final_stats.get("latest_period"))
                if final_stats.get("latest_period") else "-",
        ]
        sections.append(_data_table(
            headers=headers, rows=[row],
            col_aligns=["right", "right", "left", "left"],
            col_widths=[25, 25, 25, 25],
            caption="Source: `%s.%s.%s`" % (bq_project, bq_dataset, bq_tables["final"])))
    else:
        sections.append(_empty_state(
            "The final table was not written this run, or is empty.", "📭"))

    content_html = "".join(sections)

    subtitle = {
        "success": "All systems nominal. Data is live in BigQuery.",
        "warn":    "The batch completed, but one or more files failed. Review the audit section.",
        "danger":  "The batch did NOT complete. Investigate immediately.",
    }[header_kind]

    meta = [
        ("DAG",        dag_id),
        ("Run",        dag_run_id),
        ("Batch id",   batch_id),
        ("Spark run id", run_id or "(not available)"),
        ("Started",    _fmt_dt(started_at)),
        ("Finished",   _fmt_dt(finished_at)),
    ]

    return _shell(
        subtitle=subtitle,
        content_html=content_html,
        header_title=header_title,
        header_icon=header_icon,
        header_kind=header_kind,
        meta_rows=meta,
        env_label=env_label,
    )


# =====================================================================
# SMTP sender
# =====================================================================
def send_html_email(smtp_cfg, to_list, cc_list, subject, html_body, text_fallback=None):
    """
    Send a multipart/alternative email over STARTTLS.

    smtp_cfg: dict with smtp_host, smtp_port, smtp_user, smtp_password,
              from_address, from_name.
    """
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = formataddr((smtp_cfg.get("from_name") or "", smtp_cfg["from_address"]))
    msg["To"] = ", ".join(to_list)
    if cc_list:
        msg["Cc"] = ", ".join(cc_list)
    msg["Message-ID"] = make_msgid(domain="suki.ai")

    if not text_fallback:
        # Minimal text fallback for clients that refuse HTML.
        text_fallback = ("This email requires an HTML-capable client. "
                         "Subject: %s" % subject)
    msg.attach(MIMEText(text_fallback, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    recipients = list(to_list) + list(cc_list or [])

    context = ssl.create_default_context()
    with smtplib.SMTP(smtp_cfg["smtp_host"], int(smtp_cfg["smtp_port"]), timeout=60) as s:
        s.ehlo()
        s.starttls(context=context)
        s.ehlo()
        s.login(smtp_cfg["smtp_user"], smtp_cfg["smtp_password"])
        s.sendmail(smtp_cfg["from_address"], recipients, msg.as_string())