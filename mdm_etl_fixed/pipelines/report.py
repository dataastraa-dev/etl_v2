"""
pipelines/report.py
Generate a downloadable PDF or CSV ETL report from the pipeline_report dict
stored in etl_run_log.
"""
from __future__ import annotations

import csv
import io
import json
from datetime import datetime
from typing import Any


# ── CSV report ────────────────────────────────────────────────────────────

def generate_csv_report(pipeline_report: dict) -> bytes:
    """
    Flatten the pipeline report into a multi-section CSV.
    Returns UTF-8 encoded bytes.
    """
    output = io.StringIO()
    w = csv.writer(output)

    # ── Section 1: Summary ─────────────────────────────────────────────────
    w.writerow(["SECTION", "MDM ETL — Pipeline Report"])
    w.writerow(["Batch ID",      pipeline_report.get("batch_id", "—")])
    w.writerow(["Client ID",     pipeline_report.get("client_id", "—")])
    w.writerow(["Use Case",      pipeline_report.get("use_case_id", "—")])
    w.writerow(["Config ID",     pipeline_report.get("config_id", "—")])
    w.writerow(["Started At",    pipeline_report.get("started_at", "—")])
    w.writerow(["Completed At",  pipeline_report.get("completed_at", "—")])
    w.writerow(["File",          pipeline_report.get("extract", {}).get("file_name", "—")])
    w.writerow(["Rows Extracted", pipeline_report.get("extract", {}).get("rows_extracted", 0)])
    w.writerow(["Rows Processed", pipeline_report.get("transform", {}).get("rows_processed", 0)])

    severity_counts = pipeline_report.get("severity_counts", {})
    for sev, cnt in severity_counts.items():
        w.writerow([f"Severity — {sev}", cnt])

    if pipeline_report.get("error"):
        w.writerow(["Pipeline Error", pipeline_report["error"]])

    w.writerow([])

    # ── Section 1b: Diagnostic — why empty? ───────────────────────────────
    anomalies  = pipeline_report.get("anomalies", [])
    transforms = pipeline_report.get("transformations", [])
    phases     = pipeline_report.get("transform", {}).get("phases", {})
    user_cols  = pipeline_report.get("transform", {}).get("user_columns", [])
    rules_run  = pipeline_report.get("transform", {}).get("rows_processed", 0)

    diag_lines = _build_diagnostics(pipeline_report)
    if diag_lines:
        w.writerow(["SECTION", "Pipeline Diagnostics"])
        for line in diag_lines:
            w.writerow([line])
        # ── Section 2: Pipeline Steps ──────────────────────────────────────────
    w.writerow(["SECTION", "Pipeline Steps"])
    w.writerow(["Step", "Status", "Detail"])
    for step in pipeline_report.get("steps", []):
        detail = {k: v for k, v in step.items() if k not in ("name", "status")}
        w.writerow([step.get("name", ""), step.get("status", ""), json.dumps(detail)])

    w.writerow([])

    # ── Section 3: Anomalies ───────────────────────────────────────────────
    w.writerow(["SECTION", f"Anomalies ({len(anomalies)} total)"])
    if anomalies:
        # Dynamic headers from all keys present across anomalies
        all_keys = _union_keys(anomalies)
        preferred = ["rule", "severity", "column", "message", "affected_rows"]
        headers = preferred + [k for k in all_keys if k not in preferred]
        w.writerow(headers)
        for a in anomalies:
            w.writerow([_fmt(a.get(h)) for h in headers])
    else:
        w.writerow(["(no anomalies)"])

    w.writerow([])

    # ── Section 3b: Patches Applied ────────────────────────────────────────
    patches = pipeline_report.get("patches", [])
    w.writerow(["SECTION", f"Patches Applied ({len(patches)} total)"])
    if patches:
        all_keys = _union_keys(patches)
        preferred = ["table", "column", "staging_id", "patched_value", "reason"]
        headers = preferred + [k for k in all_keys if k not in preferred]
        w.writerow(headers)
        for p in patches:
            w.writerow([_fmt(p.get(h)) for h in headers])
    else:
        w.writerow(["(no patches applied)"])

    w.writerow([])

    # ── Section 4: Transformations applied ────────────────────────────────
    w.writerow(["SECTION", f"Transformations Applied ({len(transforms)} actions)"])
    if transforms:
        all_keys = _union_keys(transforms)
        preferred = ["phase", "strategy", "column", "severity", "message"]
        headers = preferred + [k for k in all_keys if k not in preferred]
        w.writerow(headers)
        for t in transforms:
            w.writerow([_fmt(t.get(h)) for h in headers])
    else:
        w.writerow(["(no transformation actions recorded)"])

    w.writerow([])

    # ── Section 5: Phase breakdown ─────────────────────────────────────────
    phases = pipeline_report.get("transform", {}).get("phases", {})
    if phases:
        w.writerow(["SECTION", "Phase Breakdown"])
        w.writerow(["Phase", "Strategy", "Column", "Anomaly Count"])
        for phase_name, entries in phases.items():
            for e in entries:
                w.writerow([
                    phase_name,
                    e.get("strategy", ""),
                    e.get("column", "—"),
                    e.get("anomaly_count", 0),
                ])

    return output.getvalue().encode("utf-8-sig")   # BOM for Excel compatibility


# ── PDF report ────────────────────────────────────────────────────────────

def generate_pdf_report(pipeline_report: dict) -> bytes:
    """
    Generate a PDF report using reportlab if available,
    otherwise fall back to an HTML-styled report rendered as bytes.
    """
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
        from reportlab.lib.units import cm
        from reportlab.platypus import (
            HRFlowable, PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table,
            TableStyle,
        )
        return _pdf_reportlab(pipeline_report)
    except ImportError:
        # Fallback: return HTML as bytes (browser-printable)
        return _pdf_html_fallback(pipeline_report).encode("utf-8")


def _pdf_reportlab(pipeline_report: dict) -> bytes:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import cm
    from reportlab.platypus import (
        HRFlowable, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle,
    )

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4,
                            leftMargin=2*cm, rightMargin=2*cm,
                            topMargin=2*cm, bottomMargin=2*cm)
    styles = getSampleStyleSheet()
    story  = []

    H1 = styles["Heading1"]
    H2 = styles["Heading2"]
    NM = styles["Normal"]

    ACCENT = colors.HexColor("#3b7eff")
    DANGER = colors.HexColor("#ff4d6a")
    WARN   = colors.HexColor("#f5a623")
    OK     = colors.HexColor("#4ade80")
    MUTED  = colors.HexColor("#5a6278")

    def sev_color(sev):
        s = str(sev).upper()
        if s in ("CRITICAL", "ERROR"):  return DANGER
        if s in ("FLAGGED", "WARNING"): return WARN
        return OK

    # ── Title ──────────────────────────────────────────────────────────────
    story.append(Paragraph("MDM ETL — Pipeline Report", H1))
    story.append(HRFlowable(width="100%", thickness=1, color=ACCENT))
    story.append(Spacer(1, 0.3*cm))

    # ── Summary table ──────────────────────────────────────────────────────
    sev_counts = pipeline_report.get("severity_counts", {})
    summary_data = [
        ["Field", "Value"],
        ["Batch ID",      pipeline_report.get("batch_id", "—")],
        ["Client",        pipeline_report.get("client_id", "—")],
        ["Use Case",      pipeline_report.get("use_case_id", "—")],
        ["Config ID",     pipeline_report.get("config_id", "—")],
        ["Started At",    pipeline_report.get("started_at", "—")],
        ["Completed At",  pipeline_report.get("completed_at", "—")],
        ["File",          pipeline_report.get("extract", {}).get("file_name", "—")],
        ["Rows Extracted",str(pipeline_report.get("extract", {}).get("rows_extracted", 0))],
        ["Rows Processed",str(pipeline_report.get("transform", {}).get("rows_processed", 0))],
        ["Total Anomalies",str(len(pipeline_report.get("anomalies", [])))],
        ["Total Patches Applied",str(len(pipeline_report.get("patches", [])))],
    ]
    for sev, cnt in sev_counts.items():
        summary_data.append([f"  {sev}", str(cnt)])

    if pipeline_report.get("error"):
        summary_data.append(["Pipeline Error", str(pipeline_report["error"])])

    t = Table(summary_data, colWidths=[5*cm, None])
    t.setStyle(TableStyle([
        ("BACKGROUND",  (0, 0), (-1, 0),  ACCENT),
        ("TEXTCOLOR",   (0, 0), (-1, 0),  colors.white),
        ("FONTNAME",    (0, 0), (-1, 0),  "Helvetica-Bold"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.HexColor("#f8f9fb"), colors.white]),
        ("GRID",        (0, 0), (-1, -1), 0.3, colors.HexColor("#d0d4dc")),
        ("FONTSIZE",    (0, 0), (-1, -1), 8),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING",(0, 0), (-1, -1), 6),
        ("TOPPADDING",  (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING",(0,0), (-1, -1), 4),
    ]))
    story.append(t)
    story.append(Spacer(1, 0.5*cm))

    # ── Diagnostics section ────────────────────────────────────────────────
    diag_lines = _build_diagnostics(pipeline_report)
    if diag_lines:
        story.append(Paragraph("Pipeline Diagnostics", H2))
        story.append(HRFlowable(width="100%", thickness=0.5, color=MUTED))
        story.append(Spacer(1, 0.2*cm))
        for line in diag_lines:
            bullet_style = ParagraphStyle(
                "diag", parent=NM, leftIndent=12, fontSize=8,
                textColor=colors.HexColor("#f5a623"),
                spaceBefore=2, spaceAfter=2,
            )
            story.append(Paragraph(f"⚠ {line}", bullet_style))
        story.append(Spacer(1, 0.4*cm))

    # ── Anomalies section ──────────────────────────────────────────────────
    anomalies = pipeline_report.get("anomalies", [])
    story.append(Paragraph(f"Anomalies ({len(anomalies)})", H2))
    story.append(HRFlowable(width="100%", thickness=0.5, color=MUTED))
    story.append(Spacer(1, 0.2*cm))

    if anomalies:
        headers = ["Rule", "Severity", "Column", "Message", "Affected Rows"]
        rows_data = [headers]
        for a in anomalies:
            affected = a.get("affected_rows", [])
            affected_str = f"{len(affected)} rows" if isinstance(affected, list) else str(affected)
            rows_data.append([
                str(a.get("rule", "")),
                str(a.get("severity", "")),
                str(a.get("column", "—")),
                str(a.get("message", ""))[:120],
                affected_str,
            ])

        col_widths = [3.5*cm, 2.2*cm, 3*cm, None, 2*cm]
        at = Table(rows_data, colWidths=col_widths, repeatRows=1)
        style = [
            ("BACKGROUND",  (0, 0), (-1, 0),  colors.HexColor("#1e2330")),
            ("TEXTCOLOR",   (0, 0), (-1, 0),  colors.white),
            ("FONTNAME",    (0, 0), (-1, 0),  "Helvetica-Bold"),
            ("FONTSIZE",    (0, 0), (-1, -1), 7),
            ("GRID",        (0, 0), (-1, -1), 0.3, colors.HexColor("#d0d4dc")),
            ("LEFTPADDING", (0, 0), (-1, -1), 4),
            ("RIGHTPADDING",(0, 0), (-1, -1), 4),
            ("TOPPADDING",  (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING",(0,0), (-1, -1), 3),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.HexColor("#f8f9fb"), colors.white]),
            ("WORDWRAP",    (3, 1), (3, -1),  True),
        ]
        # Colour severity column cells
        for i, a in enumerate(anomalies, start=1):
            c = sev_color(a.get("severity", ""))
            style.append(("TEXTCOLOR", (1, i), (1, i), c))
            style.append(("FONTNAME",  (1, i), (1, i), "Helvetica-Bold"))
        at.setStyle(TableStyle(style))
        story.append(at)
    else:
        story.append(Paragraph("✓ No anomalies detected.", NM))

    story.append(Spacer(1, 0.5*cm))

    # ── Patches Applied section ──────────────────────────────────────────
    patches = pipeline_report.get("patches", [])
    story.append(Paragraph(f"Patches Applied ({len(patches)})", H2))
    story.append(HRFlowable(width="100%", thickness=0.5, color=MUTED))
    story.append(Spacer(1, 0.2*cm))

    if patches:
        headers = ["Table", "Column", "Staging ID", "Patched Value", "Reason"]
        rows_data = [headers]
        for p in patches:
            rows_data.append([
                str(p.get("table", "")),
                str(p.get("column", "")),
                str(p.get("staging_id", ""))[:20],
                str(p.get("patched_value", "")),
                str(p.get("reason", ""))[:80],
            ])

        col_widths = [3.5*cm, 2.5*cm, 3.5*cm, 3*cm, None]
        pt = Table(rows_data, colWidths=col_widths, repeatRows=1)
        pt.setStyle(TableStyle([
            ("BACKGROUND",  (0, 0), (-1, 0),  colors.HexColor("#1e2330")),
            ("TEXTCOLOR",   (0, 0), (-1, 0),  colors.white),
            ("FONTNAME",    (0, 0), (-1, 0),  "Helvetica-Bold"),
            ("FONTSIZE",    (0, 0), (-1, -1), 7),
            ("GRID",        (0, 0), (-1, -1), 0.3, colors.HexColor("#d0d4dc")),
            ("LEFTPADDING", (0, 0), (-1, -1), 4),
            ("RIGHTPADDING",(0, 0), (-1, -1), 4),
            ("TOPPADDING",  (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING",(0,0), (-1, -1), 3),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.HexColor("#fff8e6"), colors.white]),
            ("WORDWRAP",    (4, 1), (4, -1),  True),
        ]))
        story.append(pt)
    else:
        story.append(Paragraph("✓ No patches were applied.", NM))

    story.append(Spacer(1, 0.5*cm))

    # ── Transformations section ────────────────────────────────────────────
    transforms = pipeline_report.get("transformations", [])
    story.append(Paragraph(f"Transformations Applied ({len(transforms)})", H2))
    story.append(HRFlowable(width="100%", thickness=0.5, color=MUTED))
    story.append(Spacer(1, 0.2*cm))

    if transforms:
        headers = ["Phase", "Strategy", "Column", "Severity", "Message"]
        rows_data = [headers]
        for t_ in transforms:
            rows_data.append([
                str(t_.get("phase", "")),
                str(t_.get("strategy", "")),
                str(t_.get("column", "—")),
                str(t_.get("severity", "INFO")),
                str(t_.get("message", ""))[:120],
            ])
        tt = Table(rows_data, colWidths=[3*cm, 3.5*cm, 2.5*cm, 2.2*cm, None], repeatRows=1)
        tt.setStyle(TableStyle([
            ("BACKGROUND",  (0, 0), (-1, 0),  colors.HexColor("#1e2330")),
            ("TEXTCOLOR",   (0, 0), (-1, 0),  colors.white),
            ("FONTNAME",    (0, 0), (-1, 0),  "Helvetica-Bold"),
            ("FONTSIZE",    (0, 0), (-1, -1), 7),
            ("GRID",        (0, 0), (-1, -1), 0.3, colors.HexColor("#d0d4dc")),
            ("LEFTPADDING", (0, 0), (-1, -1), 4),
            ("RIGHTPADDING",(0, 0), (-1, -1), 4),
            ("TOPPADDING",  (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING",(0,0), (-1, -1), 3),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.HexColor("#f8f9fb"), colors.white]),
        ]))
        story.append(tt)
    else:
        story.append(Paragraph("(no transformation actions recorded)", NM))

    story.append(Spacer(1, 0.5*cm))

    # ── Phase breakdown ────────────────────────────────────────────────────
    phases = pipeline_report.get("transform", {}).get("phases", {})
    if phases:
        story.append(Paragraph("Phase Breakdown", H2))
        story.append(HRFlowable(width="100%", thickness=0.5, color=MUTED))
        story.append(Spacer(1, 0.2*cm))
        ph_data = [["Phase", "Strategy", "Column", "Anomalies"]]
        for pname, entries in phases.items():
            for e in entries:
                ph_data.append([
                    pname, e.get("strategy", ""), e.get("column", "—"),
                    str(e.get("anomaly_count", 0)),
                ])
        pht = Table(ph_data, colWidths=[4*cm, 5*cm, 4*cm, 2*cm], repeatRows=1)
        pht.setStyle(TableStyle([
            ("BACKGROUND",  (0, 0), (-1, 0),  ACCENT),
            ("TEXTCOLOR",   (0, 0), (-1, 0),  colors.white),
            ("FONTNAME",    (0, 0), (-1, 0),  "Helvetica-Bold"),
            ("FONTSIZE",    (0, 0), (-1, -1), 7),
            ("GRID",        (0, 0), (-1, -1), 0.3, colors.HexColor("#d0d4dc")),
            ("LEFTPADDING", (0, 0), (-1, -1), 4),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.HexColor("#f8f9fb"), colors.white]),
        ]))
        story.append(pht)

    doc.build(story)
    return buf.getvalue()


def _pdf_html_fallback(pipeline_report: dict) -> str:
    """HTML fallback when reportlab is not installed."""
    def esc(v):
        return str(v).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

    anomalies  = pipeline_report.get("anomalies", [])
    transforms = pipeline_report.get("transformations", [])
    patches    = pipeline_report.get("patches", [])
    sev_counts = pipeline_report.get("severity_counts", {})
    diag_lines = _build_diagnostics(pipeline_report)

    diag_html = ""
    if diag_lines:
        items = "".join(f"<li>{esc(l)}</li>" for l in diag_lines)
        diag_html = f"<h2>Pipeline Diagnostics</h2><ul style='color:#f5a623'>{items}</ul>"

    anom_rows = "".join(
        f"<tr><td>{esc(a.get('rule',''))}</td>"
        f"<td class='sev-{esc(a.get('severity','info')).lower()}'>{esc(a.get('severity',''))}</td>"
        f"<td>{esc(a.get('column','—'))}</td>"
        f"<td>{esc(a.get('message',''))}</td>"
        f"<td>{len(a.get('affected_rows',[]) if isinstance(a.get('affected_rows'), list) else [])}</td></tr>"
        for a in anomalies
    )

    tx_rows = "".join(
        f"<tr><td>{esc(t.get('phase',''))}</td><td>{esc(t.get('strategy',''))}</td>"
        f"<td>{esc(t.get('column','—'))}</td><td>{esc(t.get('severity',''))}</td>"
        f"<td>{esc(t.get('message',''))}</td></tr>"
        for t in transforms
    )

    patch_rows = "".join(
        f"<tr><td>{esc(p.get('table',''))}</td><td>{esc(p.get('column',''))}</td>"
        f"<td>{esc(p.get('staging_id',''))}</td><td>{esc(p.get('patched_value',''))}</td>"
        f"<td>{esc(p.get('reason',''))}</td></tr>"
        for p in patches
    )

    sev_html = "".join(f"<li>{esc(k)}: <b>{v}</b></li>" for k, v in sev_counts.items())

    return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8">
<title>ETL Report — {esc(pipeline_report.get('batch_id',''))}</title>
<style>
  body{{font-family:Arial,sans-serif;font-size:12px;margin:24px;color:#1e2330}}
  h1{{color:#3b7eff;border-bottom:2px solid #3b7eff;padding-bottom:6px}}
  h2{{color:#1e2330;margin-top:24px}}
  table{{border-collapse:collapse;width:100%;margin-top:8px;font-size:11px}}
  th{{background:#1e2330;color:#fff;padding:6px 8px;text-align:left}}
  td{{padding:5px 8px;border:1px solid #d0d4dc}}
  tr:nth-child(even){{background:#f8f9fb}}
  .sev-critical,.sev-error{{color:#ff4d6a;font-weight:bold}}
  .sev-flagged,.sev-warning{{color:#f5a623;font-weight:bold}}
  dl{{display:grid;grid-template-columns:180px 1fr;gap:4px 12px}}
  dt{{font-weight:bold;color:#5a6278}}
  @media print{{body{{margin:0}} h2{{page-break-before:auto}}}}
</style></head><body>
<h1>MDM ETL — Pipeline Report</h1>
<dl>
  <dt>Batch ID</dt><dd>{esc(pipeline_report.get('batch_id','—'))}</dd>
  <dt>Client</dt><dd>{esc(pipeline_report.get('client_id','—'))}</dd>
  <dt>Use Case</dt><dd>{esc(pipeline_report.get('use_case_id','—'))}</dd>
  <dt>Config ID</dt><dd>{esc(pipeline_report.get('config_id','—'))}</dd>
  <dt>Started At</dt><dd>{esc(pipeline_report.get('started_at','—'))}</dd>
  <dt>Completed At</dt><dd>{esc(pipeline_report.get('completed_at','—'))}</dd>
  <dt>File</dt><dd>{esc(pipeline_report.get('extract',{}).get('file_name','—'))}</dd>
  <dt>Rows Extracted</dt><dd>{pipeline_report.get('extract',{}).get('rows_extracted',0)}</dd>
  <dt>Rows Processed</dt><dd>{pipeline_report.get('transform',{}).get('rows_processed',0)}</dd>
  <dt>Total Anomalies</dt><dd>{len(anomalies)}</dd>
  <dt>Total Patches Applied</dt><dd>{len(patches)}</dd>
</dl>
<ul>{sev_html}</ul>

{diag_html}

<h2>Anomalies ({len(anomalies)})</h2>
<table><tr><th>Rule</th><th>Severity</th><th>Column</th><th>Message</th><th>Affected Rows</th></tr>
{anom_rows or '<tr><td colspan="5">No anomalies detected.</td></tr>'}</table>

<h2>Patches Applied ({len(patches)})</h2>
<table><tr><th>Table</th><th>Column</th><th>Staging ID</th><th>Patched Value</th><th>Reason</th></tr>
{patch_rows or '<tr><td colspan="5">No patches were applied.</td></tr>'}</table>

<h2>Transformations Applied ({len(transforms)})</h2>
<table><tr><th>Phase</th><th>Strategy</th><th>Column</th><th>Severity</th><th>Message</th></tr>
{tx_rows or '<tr><td colspan="5">No transformation actions recorded.</td></tr>'}</table>
</body></html>"""


def _build_diagnostics(pipeline_report: dict) -> list[str]:
    """
    Return human-readable lines explaining why anomalies or transformations
    might be zero — so the report is self-explanatory.
    """
    lines = []
    anomalies  = pipeline_report.get("anomalies", [])
    transforms = pipeline_report.get("transformations", [])
    phases     = pipeline_report.get("transform", {}).get("phases", {})
    user_cols  = pipeline_report.get("transform", {}).get("user_columns", [])
    rows       = pipeline_report.get("transform", {}).get("rows_processed", 0)

    if rows == 0:
        lines.append("No rows were processed — check that the CSV was uploaded correctly.")
        return lines

    # Global transformers
    # GLOBAL_TRANSFORMERS list is intentionally empty until strategies/transformations.py
    # registers strategies via @StrategyRegistry.register. No warning needed.
    gt_ran = [e["strategy"] for e in phases.get("global_transform", [])]

    # Global validators
    gv_ran = [e["strategy"] for e in phases.get("global_validate", [])]
    gv_anomalies = sum(e.get("anomaly_count", 0) for e in phases.get("global_validate", []))
    if gv_anomalies == 0 and gv_ran:
        lines.append(
            f"Global validators ({', '.join(gv_ran)}) ran but found 0 issues — "
            "this may be correct, or column names in the CSV may differ from expected "
            "(e.g. 'Cost' vs 'cost'). Column mapping is only applied if configured."
        )

    # User-defined rules
    uv_rules = [e for e in phases.get("user_validate", [])]
    ut_rules = [e for e in phases.get("user_transform", [])]
    if not uv_rules and not ut_rules:
        lines.append(
            "No user-defined validations (UV) or transformations (UT) were applied. "
            "Assign UV/UT rules to columns in the Rule Config UI and commit before running."
        )
    else:
        if not uv_rules:
            lines.append("No UV rules ran — none were assigned in the committed config.")
        else:
            uv_anomalies = sum(e.get("anomaly_count", 0) for e in uv_rules)
            if uv_anomalies == 0:
                lines.append(
                    f"UV rules ran on columns {user_cols} but found 0 anomalies. "
                    "Verify that 'parameters' (e.g. min/max for numeric_range) are set in the rule config — "
                    "empty parameters mean no bounds are enforced."
                )

    if not anomalies and not lines:
        lines.append("All validations passed cleanly — data meets every configured rule.")

    return lines


# ── Utilities ─────────────────────────────────────────────────────────────

def _union_keys(records: list[dict]) -> list[str]:
    seen: dict[str, None] = {}
    for r in records:
        for k in r:
            seen[k] = None
    return list(seen)


def _fmt(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, list):
        return f"[{len(v)} rows]"
    return str(v)