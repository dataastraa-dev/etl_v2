"""
api/routes.py
Flask blueprint — all v1 API endpoints.
 
New endpoints added:
    POST  /v1/batches/run               Accept CSV + config_id, run full ETL
    GET   /v1/batches/<id>/report       Download PDF or CSV report
"""
import io
import json
import threading
import traceback
import re
from datetime import date, datetime
from flask import request, jsonify
from core.database import get_conn

from flask import Blueprint, request, jsonify, send_file
 
from core.database import get_conn
from database.repositories import (
    BatchMasterRepository,
    EtlRunLogRepository,
    PipelineConfigRepository,
    GoldenSchemaRegistryRepository,
    GoldenTableRepository,
    PipelineConfigRepository,
)
from pipelines.orchestrator import run_pipeline
 
bridge_bp = Blueprint("api", __name__)
 
 
# ── Existing: trigger batch (JSON-only, no file) ───────────────────────────
 
@bridge_bp.route("/batches", methods=["POST"])
def trigger_batch():
    """
    Trigger a new ETL pipeline run (legacy — no CSV upload).
    Expects JSON: { "client_id": "...", "use_case_id": "...", "config_id": "..." }
    """
    payload = request.get_json()
 
    client_id   = payload.get("client_id")
    use_case_id = payload.get("use_case_id")
    config_id   = payload.get("config_id")
    run_by      = payload.get("run_by", "api_trigger")
 
    if not all([client_id, use_case_id, config_id]):
        return jsonify({"error": "client_id, use_case_id, and config_id are required."}), 400
 
    try:
        conn       = get_conn()
        batch_repo = BatchMasterRepository(conn)
 
        batch_id = batch_repo.create(
            config_id=config_id,
            run_by=run_by,
        )
 
        thread = threading.Thread(
            target=run_pipeline,
            args=(batch_id, client_id, use_case_id, config_id),
            daemon=True,
        )
        thread.start()
 
        return jsonify({
            "batch_id": batch_id,
            "status":   "PENDING",
            "message":  "Batch processing initiated.",
        }), 202
 
    except Exception as e:
        return jsonify({"error": str(e)}), 500
 
 
# ── NEW: Run ETL with CSV file upload ─────────────────────────────────────
 
@bridge_bp.route("/batches/run", methods=["POST"])
def run_etl():
    """
    Accept a CSV file + config_id and run the full ETL pipeline synchronously.
    The response is returned after the pipeline completes (suitable for small–medium files).
    For large files the client can use the async /batches endpoint + polling.
 
    Form data:
        file        (required)  — CSV file
        config_id   (required)  — UUID of committed config
        client_id   (required)
        use_case_id (required)
        run_by      (optional)  — default "ui_upload"
        dataset_type (optional) — default "csv_upload"
        table_name   (optional) — default "csv_upload"
        async        (optional) — if "true", runs in background and returns batch_id immediately
    """
    # ── Validate inputs ────────────────────────────────────────────────────
    csv_file    = request.files.get("file")
    client_id   = request.form.get("client_id")
    use_case_id = request.form.get("use_case_id")
    config_id   = request.form.get("config_id")
    run_by      = request.form.get("run_by", "ui_upload")
    dataset_type = request.form.get("dataset_type", "csv_upload")
    table_name   = request.form.get("table_name", "csv_upload")
    is_async      = request.form.get("async", "false").lower() == "true"
    use_case_name = request.form.get("use_case_name", "").strip() or None
 
    if not csv_file:
        return jsonify({"error": "A CSV file is required (field name: 'file')."}), 400
    if not all([client_id, use_case_id, config_id]):
        return jsonify({"error": "client_id, use_case_id, and config_id are required."}), 400
 
    file_name = csv_file.filename or "upload.csv"
    csv_bytes = csv_file.read()   # read into memory so thread-safe
 
    try:
        conn       = get_conn()
        batch_repo = BatchMasterRepository(conn)
 
        batch_id = batch_repo.create(
            config_id=config_id,
            run_by=run_by,
        )
    except Exception as e:
        return jsonify({"error": f"Could not create batch: {e}"}), 500
 
    if is_async:
        # Fire and forget — caller polls /batches/<id>/status
        thread = threading.Thread(
            target=run_pipeline,
            kwargs=dict(
                batch_id=batch_id,
                client_id=client_id,
                use_case_id=use_case_id,
                config_id=config_id,
                csv_file=csv_bytes,
                file_name=file_name,
                dataset_type=dataset_type,
                table_name=table_name,
                use_case_name=use_case_name,
            ),
            daemon=True,
        )
        thread.start()
        return jsonify({
            "batch_id":    batch_id,
            "status":      "PENDING",
            "message":     "ETL started asynchronously. Poll /v1/batches/<id>/status for updates.",
            "report_url":  f"/v1/batches/{batch_id}/report",
        }), 202
    else:
        # Synchronous — run in-request and return full report summary
        try:
            report = run_pipeline(
                batch_id=batch_id,
                client_id=client_id,
                use_case_id=use_case_id,
                config_id=config_id,
                csv_file=csv_bytes,
                file_name=file_name,
                dataset_type=dataset_type,
                table_name=table_name,
            )
        except Exception as e:
            tb = traceback.format_exc()
            print(f"[ETL ERROR] {tb}")
            return jsonify({"error": str(e), "traceback": tb, "batch_id": batch_id}), 500
 
        status = "FAILED" if report.get("error") else "COMPLETED"
        return jsonify({
            "batch_id":        batch_id,
            "status":          status,
            "rows_extracted":  report.get("extract", {}).get("rows_extracted", 0),
            "rows_processed":  report.get("transform", {}).get("rows_processed", 0),
            "anomaly_count":   len(report.get("anomalies", [])),
            "severity_counts": report.get("severity_counts", {}),
            "error":           report.get("error"),
            "report_url":      f"/v1/batches/{batch_id}/report",
        }), 200 if status == "COMPLETED" else 500


# ── NEW: Hot Folder ingest (on-demand, mirrors auto_ingest.py's watcher) ───
import os
import shutil
import time

@bridge_bp.route("/hotfolder/ingest", methods=["POST"])
def hotfolder_ingest():
    """
    Scan a server-side folder for *.csv files and run the ETL pipeline for
    each one found, exactly like auto_ingest.py's watchdog handler — but
    triggered on demand from the UI instead of by a filesystem event.

    Each filename must follow the existing convention:
        clientID__useCaseName__timestamp.csv

    On success, the file is moved to <folder_path>/processed/.
    On failure (bad filename, no active config, pipeline error), the file
    is moved to <folder_path>/failed/ and the error is reported back.

    JSON body:
        folder_path (required) — absolute or relative path to the hot folder

    Returns a list of per-file results, one batch per CSV found.
    """
    body = request.get_json(silent=True) or {}
    folder_path = (body.get("folder_path") or "").strip()

    if not folder_path:
        return jsonify({"error": "folder_path is required."}), 400
    if not os.path.isdir(folder_path):
        return jsonify({"error": f"Folder not found: {folder_path!r}. "
                                  f"Check the path is correct and accessible to the server process."}), 400

    processed_dir = os.path.join(folder_path, "processed")
    failed_dir    = os.path.join(folder_path, "failed")
    os.makedirs(processed_dir, exist_ok=True)
    os.makedirs(failed_dir, exist_ok=True)

    csv_files = sorted(
        f for f in os.listdir(folder_path)
        if f.lower().endswith(".csv") and os.path.isfile(os.path.join(folder_path, f))
    )

    if not csv_files:
        return jsonify({"folder_path": folder_path, "files_found": 0, "results": []}), 200

    results = []

    for filename in csv_files:
        filepath = os.path.join(folder_path, filename)
        result = {"file_name": filename}

        # ── Parse clientID__useCaseName__timestamp.csv ──────────────────────
        name_without_ext = os.path.splitext(filename)[0]
        parts = name_without_ext.split("__")

        if len(parts) < 2:
            result.update({
                "status": "FAILED",
                "error": "Invalid naming convention. Expected: clientID__useCaseName__timestamp.csv",
            })
            _move_hotfolder_file(filepath, failed_dir, filename)
            results.append(result)
            continue

        client_id     = parts[0]
        use_case_name = parts[1]
        result.update({"client_id": client_id, "use_case_id": use_case_name})

        try:
            conn       = get_conn()
            batch_repo = BatchMasterRepository(conn)

            # config_id=None lets BatchMasterRepository.create() auto-resolve
            # the active config for this client/use_case (raises ValueError
            # with a clear message if none exists — same as auto_ingest.py).
            batch_id = batch_repo.create(
                client_name=client_id,
                use_case_name=use_case_name,
                run_by="hotfolder_ui",
            )
            result["batch_id"] = batch_id

            report = run_pipeline(
                batch_id=batch_id,
                client_id=client_id,
                use_case_id=use_case_name,
                config_id=None,
                csv_file=filepath,          # extract_data accepts a path string directly
                file_name=filename,
            )

            if report.get("error"):
                result.update({"status": "FAILED", "error": report["error"]})
                _move_hotfolder_file(filepath, failed_dir, filename)
            else:
                result.update({
                    "status":          "COMPLETED",
                    "rows_extracted":  report.get("extract", {}).get("rows_extracted", 0),
                    "rows_processed":  report.get("transform", {}).get("rows_processed", 0),
                    "anomaly_count":   len(report.get("anomalies", [])),
                    "severity_counts": report.get("severity_counts", {}),
                    "report_url":      f"/v1/batches/{batch_id}/report",
                })
                _move_hotfolder_file(filepath, processed_dir, filename)

        except Exception as e:
            result.update({"status": "FAILED", "error": str(e)})
            _move_hotfolder_file(filepath, failed_dir, filename)

        results.append(result)

    return jsonify({
        "folder_path": folder_path,
        "files_found": len(csv_files),
        "results":     results,
    }), 200


def _move_hotfolder_file(src_path: str, dest_dir: str, filename: str) -> None:
    """Safely move a hot-folder file, overwriting any same-named file already there."""
    dest_path = os.path.join(dest_dir, filename)
    try:
        if os.path.exists(dest_path):
            os.remove(dest_path)
        shutil.move(src_path, dest_path)
    except Exception as e:
        print(f"[HOT FOLDER UI] Could not move {filename} to {dest_dir}: {e}")
 
 
# ── NEW: Download report ───────────────────────────────────────────────────
 
@bridge_bp.route("/batches/<batch_id>/report", methods=["GET"])
def download_report(batch_id):
    """
    Download the ETL report for a batch as PDF or CSV.
 
    Query params:
        format   pdf | csv  (default: pdf)
    """
    fmt = request.args.get("format", "pdf").lower()
 
    try:
        conn       = get_conn()
        batch_repo = BatchMasterRepository(conn)
        log_repo   = EtlRunLogRepository(conn)
 
        batch_info = batch_repo.fetch(batch_id)
        if not batch_info:
            return jsonify({"error": "Batch not found."}), 404
 
        if str(batch_info.get("status")) not in ("COMPLETED", "FAILED"):
            return jsonify({
                "error": f"Report not yet available. Batch status: {batch_info.get('status')}",
                "status": batch_info.get("status"),
            }), 202
 
        # Retrieve the full pipeline report stored by the orchestrator
        pipeline_report = _fetch_pipeline_report(log_repo, batch_id)
        if not pipeline_report:
            # Fallback: reconstruct a minimal report from batch info + logs
            pipeline_report = _minimal_report(batch_info, log_repo.fetch_by_batch(batch_id))
 
    except Exception as e:
        return jsonify({"error": str(e)}), 500
 
    # ── Generate file ──────────────────────────────────────────────────────
    from pipelines.report import generate_csv_report, generate_pdf_report
 
    if fmt == "csv":
        data         = generate_csv_report(pipeline_report)
        mime         = "text/csv"
        ext          = "csv"
        content_disp = f"attachment; filename=etl_report_{batch_id[:8]}.csv"
    else:
        data = generate_pdf_report(pipeline_report)
        # Check if reportlab produced real PDF bytes or HTML fallback
        if data[:4] == b"%PDF":
            mime = "application/pdf"
            ext  = "pdf"
        else:
            mime = "text/html"
            ext  = "html"
        content_disp = f"attachment; filename=etl_report_{batch_id[:8]}.{ext}"
 
    return send_file(
        io.BytesIO(data) if isinstance(data, bytes) else io.BytesIO(data),
        mimetype=mime,
        as_attachment=True,
        download_name=f"etl_report_{batch_id[:8]}.{ext}",
    )
 
 
# ── Existing: commit config ────────────────────────────────────────────────
 
@bridge_bp.route("/configs", methods=["POST"])
def commit_config():
    """
    Persist transformation rules from the UI into client_pipeline_config.
    Expects JSON: {
        "client_id": "...",
        "use_case_id": "...",
        "column_mapping": {...},
        "transformation_rules": [...]
    }
    Returns: { "config_id": "..." }
    """
    payload = request.get_json(force=True, silent=True) or {}
 
    client_id            = payload.get("client_id")
    use_case_id          = payload.get("use_case_id")
    column_mapping       = payload.get("column_mapping", {})
    transformation_rules = payload.get("transformation_rules", [])
    patch_missing_keys   = bool(payload.get("patch_missing_keys", False))
 
    if not all([client_id, use_case_id]):
        return jsonify({"error": "client_id and use_case_id are required."}), 400
 
    if not isinstance(transformation_rules, list):
        return jsonify({"error": "transformation_rules must be an array."}), 400
 
    try:
        conn         = get_conn()
        base_repo    = PipelineConfigRepository(conn)
        schema_repo  = GoldenSchemaRegistryRepository(conn)
        config_repo  = PipelineConfigRepository(conn)
 
        base_repo.ensure_exists(client_id, use_case_id)
 
        schema_record = schema_repo.fetch_latest(use_case_id)
 
        # Pull the golden column list from the use case definition (populated
        # from the BI accelerator export JSON) so mandatory_columns is never
        # silently left empty.
        uc_repo = PipelineConfigRepository(conn)
        # use_case_id from the UI is typically already the use_case_name
        # string (e.g. "sales") — pipeline_config has no separate
        # use_case_id column, just use_case_name. Try it directly, and if
        # nothing matches, fall back to the most recently created use case
        # for this client.
        uc_def = uc_repo.fetch_active(client_id, use_case_id)
        if not uc_def:
            all_cases = uc_repo.list_for_client(client_id)
            print(f"[DIAG commit_config] client_id={client_id!r} use_case_id={use_case_id!r}", flush=True)
            print(f"[DIAG commit_config] all_cases for client: {[c.get('use_case_name') for c in all_cases]}", flush=True)
            if all_cases:
                # Last resort: use the most recently created use case for this client
                uc_def = uc_repo.fetch_active(client_id, all_cases[0]["use_case_name"])
        if not uc_def:
            print(f"[DIAG commit_config] NO use_case_definitions row found for client_id={client_id!r}, use_case_id={use_case_id!r}. "
                  f"column_mapping will be empty unless the caller supplied one explicitly.", flush=True)
        uc_required = (uc_def or {}).get("required_columns") or []
 
        if schema_record:
            schema_id = schema_record["schema_id"]
            existing_mandatory = schema_record.get("mandatory_columns") or []
            if isinstance(existing_mandatory, str):
                existing_mandatory = json.loads(existing_mandatory)
            # Backfill: if the registry has no mandatory columns yet but the
            # use case definition does, populate it now (non-destructive —
            # only fires when the registry side is still empty).
            if not existing_mandatory and uc_required:
                existing_types = schema_record.get("expected_data_types") or {}
                if isinstance(existing_types, str):
                    existing_types = json.loads(existing_types)
                schema_id = schema_repo.upsert(
                    dataset_type=use_case_id,
                    version=schema_record.get("version", "1.0"),
                    mandatory_columns=uc_required,
                    expected_data_types=existing_types,
                )
        else:
            schema_id = schema_repo.upsert(
                dataset_type=use_case_id,
                version="1.0",
                mandatory_columns=uc_required,
                expected_data_types={},
            )
 
        # If the caller sent no column_mapping (the UI always sends {}),
        # pull it from the use_case_definitions record that was populated
        # when the BI accelerator bundle was imported.  Without this, the
        # column_mapper transformer receives an empty mapping and leaves all
        # raw CSV column names in the payload, causing NULL violations at load.
        if not column_mapping and uc_def:
            column_mapping = (uc_def or {}).get("column_mapping") or {}
 
        config_id = config_repo.upsert_config(
            client_name=client_id,
            use_case_name=use_case_id,
            schema_id=schema_id,
            column_mapping=column_mapping,
            transformation_rules=transformation_rules,
            patch_missing_keys=patch_missing_keys,
        )
 
        return jsonify({"config_id": config_id, "rules_saved": len(transformation_rules)}), 201
 
    except Exception as e:
        return jsonify({"error": str(e)}), 500
 
@bridge_bp.route("/configs/active", methods=["GET"])
def get_active_config():
    """Fetches the latest active configuration for a client and use case."""
    client_id = request.args.get("client_id")
    use_case_id = request.args.get("use_case_id")
    
    if not client_id or not use_case_id:
        return jsonify({"error": "Missing client_id or use_case_id"}), 400
        
    try:
        conn = get_conn()
        repo = PipelineConfigRepository(conn)
        config = repo.fetch_active(client_id, use_case_id)
        
        if not config:
            return jsonify({"message": "No active config found"}), 404
            
        return jsonify(config), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
 
# ── Existing: batch status ─────────────────────────────────────────────────
 
@bridge_bp.route("/batches/<batch_id>/status", methods=["GET"])
def get_batch_status(batch_id):
    """Retrieve the current status and execution logs of a specific batch run."""
    try:
        conn       = get_conn()
        batch_repo = BatchMasterRepository(conn)
        log_repo   = EtlRunLogRepository(conn)
 
        batch_info = batch_repo.fetch(batch_id)
        if not batch_info:
            return jsonify({"error": "Batch not found."}), 404
 
        logs = log_repo.fetch_by_batch(batch_id)
 
        return jsonify({
            "batch_id":           str(batch_info["batch_id"]),
            "status":             batch_info["status"],
            "created_datetime":   str(batch_info["created_datetime"]),
            "completed_datetime": str(batch_info["completed_datetime"]) if batch_info["completed_datetime"] else None,
            "logs":               _safe_logs(logs),
            "report_url":         f"/v1/batches/{batch_id}/report",
        }), 200
 
    except Exception as e:
        return jsonify({"error": str(e)}), 500
 
 
# ── Helpers ───────────────────────────────────────────────────────────────
 
def _fetch_pipeline_report(log_repo, batch_id: str) -> dict | None:
    """Find the PIPELINE_COMPLETE log entry and return its detail dict."""
    logs = log_repo.fetch_by_batch(batch_id)
    for log in reversed(logs):
        if log.get("step_name") == "PIPELINE_COMPLETE":
            detail = log.get("detail")
            if isinstance(detail, str):
                try:
                    detail = json.loads(detail)
                except Exception:
                    pass
            if isinstance(detail, dict):
                return detail
    return None
 
 
def _minimal_report(batch_info: dict, logs: list) -> dict:
    """Build a minimal report dict when the full report log is missing."""
    return {
        "batch_id":       str(batch_info.get("batch_id", "—")),
        "client_id":      str(batch_info.get("client_id", "—")),
        "use_case_id":    str(batch_info.get("use_case_id", "—")),
        "config_id":      str(batch_info.get("config_id", "—")),
        "started_at":     str(batch_info.get("created_datetime", "—")),
        "completed_at":   str(batch_info.get("completed_datetime", "—")),
        "extract":        {},
        "transform":      {},
        "anomalies":      [],
        "transformations":[],
        "severity_counts":{},
        "steps":          [{"name": l.get("step_name"), "status": l.get("status")} for l in logs],
    }
 
 
def _safe_logs(logs: list) -> list:
    """Strip non-serialisable values from log dicts."""
    safe = []
    for l in logs:
        safe.append({k: str(v) if not isinstance(v, (str, int, float, bool, type(None), dict, list)) else v
                     for k, v in l.items()})
    return safe
 
 
# ── Use Case Import & List ─────────────────────────────────────────────────
 
@bridge_bp.route("/use-cases", methods=["POST"])
def import_use_case():
    """
    Import a use-case mapping bundle exported from bi_accelerator.
 
    Accepts either:
      A) JSON body:  { "client_id": "...", "use_case_name": "...", "bundle": { ...full JSON... } }
      B) Flat JSON body: the raw BI-accelerator export pasted directly (no wrapper)
         In this case client_id and use_case_name can be top-level fields.
      C) Multipart form: client_id + use_case_name fields + bundle as .json file upload
 
    The bundle normaliser handles every known BI-accelerator export shape
    so the route never 400s due to structural differences.
    """
    import re
 
    # ── 1. Parse input ────────────────────────────────────────────────────────
    if request.is_json:
        data = request.get_json(force=True) or {}
 
        # Case A: wrapped  { client_id, use_case_name, bundle: {...} }
        if "bundle" in data:
            client_id     = str(data.get("client_id", "")).strip()
            use_case_name = str(data.get("use_case_name", "")).strip()
            bundle        = data["bundle"] if isinstance(data["bundle"], dict) else {}
        else:
            # Case B: raw export pasted directly — client_id / use_case_name
            # may be top-level keys inside the export itself
            bundle        = data
            client_id     = str(data.get("client_id", "")).strip()
            use_case_name = str(data.get("use_case_name", "")).strip()
    else:
        # Case C: multipart form
        client_id     = str(request.form.get("client_id", "")).strip()
        use_case_name = str(request.form.get("use_case_name", "")).strip()
        bundle_file   = request.files.get("bundle")
        if not bundle_file:
            return jsonify({
                "error": "bundle file is required for multipart requests.",
                "hint":  "Send JSON body with {client_id, use_case_name, bundle} or upload a .json file."
            }), 400
        try:
            bundle = json.loads(bundle_file.read().decode("utf-8"))
        except Exception as e:
            return jsonify({"error": f"Could not parse bundle JSON: {e}"}), 400
 
    if not client_id:
        return jsonify({"error": "client_id is required."}), 400
 
    if not bundle:
        return jsonify({
            "error": "bundle is empty or missing.",
            "hint":  "Send: {\"client_id\": \"...\", \"use_case_name\": \"...\", \"bundle\": { ...BI accelerator export... }}"
        }), 400
 
    # ── 2. Resolve use_case_name (multiple fallback keys) ────────────────────
    raw_name = (
        use_case_name
        or str(bundle.get("use_case_name", "")).strip()
        or str(bundle.get("name", "")).strip()
        or str(bundle.get("dataset_name", "")).strip()
        or str(bundle.get("use_case", "")).strip()
    )
    if not raw_name:
        return jsonify({
            "error": "use_case_name is required.",
            "hint":  "Pass use_case_name in the request body, or ensure the bundle contains a 'use_case_name' field.",
            "bundle_top_level_keys": list(bundle.keys()),
        }), 400
 
    safe_name = re.sub(r"[^a-z0-9_]", "_", raw_name.lower().strip())
 
    # ── 3. Normalise bundle — handle every known BI accelerator shape ─────────
    tables_schema, column_mapping, required_columns, mapping_by_file = _normalise_bundle(bundle)
 
    source_client = str(bundle.get("source_client", bundle.get("client_id", ""))).strip()
    exported_at   = str(bundle.get("exported_at",   bundle.get("created_at",  ""))).strip()
 
    # ── 4. Persist ────────────────────────────────────────────────────────────
    try:
        conn    = get_conn()
        uc_repo = PipelineConfigRepository(conn)
 
        use_case_id = uc_repo.upsert_definition(
            client_name      = client_id,
            use_case_name    = safe_name,
            display_name     = raw_name,
            tables_schema    = tables_schema,
            required_columns = required_columns,
            source_client    = source_client,
            exported_at      = exported_at,
            mapping_by_file  = mapping_by_file,
        )
 
        return jsonify({
            "use_case_id":    use_case_id,
            "use_case_name":  safe_name,
            "display_name":   raw_name,
            "tables":         len(tables_schema),
            "golden_columns": len(required_columns),
            "mapped_columns": len(column_mapping),
            "mapped_files":   list(mapping_by_file.keys()),
            "message":        f"Use case '{safe_name}' imported successfully.",
        }), 201
 
    except Exception as e:
        return jsonify({"error": str(e)}), 500
 
 
def _normalise_bundle(bundle: dict) -> tuple[list, dict, list, dict]:
    """
    Convert any known BI-accelerator export shape into
    (tables_schema, column_mapping, required_columns, mapping_by_file).

    No hardcoded schema — everything comes from the bundle itself.
    The golden schema is whatever was exported from bi_accelerator
    (which queries its own golden_schema_master table dynamically).

    mapping_by_file is the per-source-filename column mapping (e.g.
    {"DK_SampleData_Apr_to_Sep.csv": {...}, "SalesRep_Master.csv": {...}}),
    present only on exports built after the bi_accelerator filename-tracking
    fix. Older exports won't have it — callers should treat {} as "unknown,
    fall back to the flat column_mapping for every uploaded file."

    Handles:
      Shape 1 (standard):  bundle.tables + bundle.column_mapping
      Shape 2 (flat cols): bundle.columns list
      Shape 3 (schema):    bundle.schema dict
      Shape 4:             bundle.golden_columns list
      Shape 5/6:           bare column_mapping or flat str→str dict
    """

    def _is_garbage_mapping(cm: dict) -> bool:
        """True if mapping keys/values look like numeric DB IDs (not column names)."""
        if not cm:
            return False
        pairs = list(cm.items())[:5]
        numeric = sum(1 for k, v in pairs if str(k).strip().isdigit() and str(v).strip().isdigit())
        return numeric >= min(3, len(pairs))

    def _clean_mapping(cm: dict) -> dict:
        """Strip entries where key or value is a plain integer string."""
        return {
            str(k): str(v)
            for k, v in cm.items()
            if not str(k).strip().isdigit() and not str(v).strip().isdigit()
        }

    def _flatten_nested_mapping(raw_cm: dict) -> dict:
        """
        Handle the bi_accelerator nested column_mapping format where the export
        bundles mappings under table-name keys, e.g.:
            { "csv_upload": { "CUSTOMER_TRX_ID": "transaction_id", ... }, ... }
        Prefers the "csv_upload" sub-dict (matches staging table_name), then
        falls back to the largest sub-dict. Returns the full nested dict so
        column_mapper can do table-scoped lookup, with a flat fallback merged in.
        """
        if not raw_cm:
            return {}
        # Prefer "csv_upload" key — that is what table_name is set to on upload
        flat = raw_cm.get("csv_upload") or max(
            (v for v in raw_cm.values() if isinstance(v, dict)),
            key=len,
            default={}
        )
        # Keep the full nested structure so column_mapper table-scoped lookup works
        result = {k: v for k, v in raw_cm.items() if isinstance(v, dict)}
        # Also merge flat entries at top level as fallback
        result.update({k: v for k, v in flat.items() if not isinstance(v, dict)})
        return result

    def _clean_mapping_by_file(raw_mbf) -> dict:
        """
        Validate/clean bundle.mapping_by_file: {filename: {src_col: golden_col}}.
        Drops anything that isn't a proper filename->dict structure, and runs
        each per-file mapping through the same garbage/numeric-ID cleaning as
        the flat mapping, so a corrupted entry can't silently poison the join.
        """
        if not isinstance(raw_mbf, dict):
            return {}
        cleaned = {}
        for fname, fmap in raw_mbf.items():
            if not isinstance(fname, str) or not fname.strip() or not isinstance(fmap, dict):
                continue
            if _is_garbage_mapping(fmap):
                continue
            cm = _clean_mapping(fmap)
            if cm:
                cleaned[fname.strip()] = cm
        return cleaned

    tables_schema    : list = []
    column_mapping   : dict = {}
    required_columns : list = []
    mapping_by_file  : dict = {}

    # ── Shape 1: standard {tables, column_mapping} ───────────────────────────
    if "tables" in bundle and isinstance(bundle["tables"], list):
        tables_schema = bundle["tables"]
        raw_cm        = bundle.get("column_mapping", {})

        if _is_garbage_mapping(raw_cm):
            column_mapping = {}
        elif any(isinstance(v, dict) for v in raw_cm.values()):
            # Nested (table-keyed) mapping from bi_accelerator export — flatten it
            column_mapping = _flatten_nested_mapping(raw_cm)
        else:
            column_mapping = _clean_mapping(raw_cm)

        mapping_by_file = _clean_mapping_by_file(bundle.get("mapping_by_file", {}))

        for tbl in tables_schema:
            for col in tbl.get("columns", []):
                gc = col.get("golden_column") or col.get("name") or col.get("column_name")
                if gc:
                    required_columns.append(gc)

        if not tables_schema:
            return [], {}, [], {}   # bundle had empty tables — caller must re-export from bi_accelerator

        if not column_mapping:
            # Derive identity mapping so ETL at least knows all required golden cols
            column_mapping = {gc: gc for gc in required_columns}

        return tables_schema, column_mapping, required_columns, mapping_by_file
 
    # ── Shape 2: flat columns list ────────────────────────────────────────────
    if "columns" in bundle and isinstance(bundle["columns"], list):
        cols      = bundle["columns"]
        tbl_name  = bundle.get("table_name", bundle.get("dataset_name", "main_table"))
        tables_schema = [{"table_name": tbl_name, "columns": cols}]
        raw_cm    = bundle.get("column_mapping", {})
        column_mapping = _clean_mapping(raw_cm) if not _is_garbage_mapping(raw_cm) else {}
        for col in cols:
            gc  = col.get("golden_column") or col.get("mapped_to") or col.get("name") or col.get("column_name")
            src = col.get("source_column") or col.get("src") or col.get("original_name") or col.get("name")
            if gc:
                required_columns.append(gc)
            if not column_mapping and src and gc and src != gc:
                column_mapping[src] = gc
        if not column_mapping:
            column_mapping = {gc: gc for gc in required_columns}
        return tables_schema, column_mapping, required_columns, {}

    # ── Shape 3: schema dict {table: [col,...]} ───────────────────────────────
    if "schema" in bundle and isinstance(bundle["schema"], dict):
        raw_cm = bundle.get("column_mapping", {})
        column_mapping = _clean_mapping(raw_cm) if not _is_garbage_mapping(raw_cm) else {}
        for tbl_name, col_list in bundle["schema"].items():
            if isinstance(col_list, list):
                col_objs = [
                    c if isinstance(c, dict)
                    else {"column_name": str(c), "golden_column": str(c)}
                    for c in col_list
                ]
                tables_schema.append({"table_name": tbl_name, "columns": col_objs})
                for c in col_objs:
                    gc = c.get("golden_column") or c.get("column_name")
                    if gc:
                        required_columns.append(gc)
        if not column_mapping:
            column_mapping = {gc: gc for gc in required_columns}
        return tables_schema, column_mapping, required_columns, {}

    # ── Shape 4: golden_schema_master style ───────────────────────────────────
    if "golden_columns" in bundle and isinstance(bundle["golden_columns"], list):
        gcols    = bundle["golden_columns"]
        tbl_name = bundle.get("table_name", "golden_table")
        col_objs = [
            {"column_name": c.get("column_name", ""),
             "golden_column": c.get("column_name", ""),
             "data_type": c.get("data_type", "")}
            for c in gcols
        ]
        tables_schema    = [{"table_name": tbl_name, "columns": col_objs}]
        raw_cm           = bundle.get("column_mapping", {})
        column_mapping   = _clean_mapping(raw_cm) if not _is_garbage_mapping(raw_cm) else {}
        required_columns = [c["column_name"] for c in col_objs if c["column_name"]]
        if not column_mapping:
            column_mapping = {gc: gc for gc in required_columns}
        return tables_schema, column_mapping, required_columns, {}

    # ── Shape 5/6: bare column_mapping or flat str→str ────────────────────────
    raw_cm = bundle.get("column_mapping", {})
    if not raw_cm and all(isinstance(v, str) for v in bundle.values()):
        raw_cm = bundle   # bundle itself is the mapping
    if raw_cm and not _is_garbage_mapping(raw_cm):
        column_mapping   = _clean_mapping(raw_cm)
        required_columns = list(column_mapping.values())
        tbl_name         = bundle.get("table_name", "mapped_table")
        tables_schema    = [{"table_name": tbl_name,
                             "columns": [{"source_column": k, "golden_column": v}
                                         for k, v in column_mapping.items()]}]
        return tables_schema, column_mapping, required_columns, {}

    # ── Nothing usable — return empty so caller can reject ────────────────────
    return [], {}, [], {}
 
@bridge_bp.route("/use-cases/debug", methods=["POST"])
def debug_bundle():
    """
    Debug endpoint — accepts any JSON and returns what the normaliser
    would produce without writing anything to the DB.
    Useful when the import 400s and you need to see the parsed shape.
    """
    data   = request.get_json(force=True) or {}
    bundle = data.get("bundle", data)  # accept wrapped or raw
 
    tables_schema, column_mapping, required_columns, mapping_by_file = _normalise_bundle(bundle)
 
    return jsonify({
        "bundle_top_level_keys": list(bundle.keys()),
        "normalised": {
            "tables":          len(tables_schema),
            "table_names":     [t.get("table_name") for t in tables_schema],
            "golden_columns":  len(required_columns),
            "mapped_columns":  len(column_mapping),
            "column_mapping_sample": dict(list(column_mapping.items())[:5]),
            "required_columns_sample": required_columns[:10],
            "mapped_files":    list(mapping_by_file.keys()),
            "mapping_by_file_sample": {
                k: dict(list(v.items())[:5]) for k, v in list(mapping_by_file.items())[:3]
            },
        },
    }), 200
 
 
@bridge_bp.route("/use-cases", methods=["GET"])
def list_use_cases():
    """
    List all use cases for a client.
    Query param: client_id (required)
    """
    client_id = request.args.get("client_id", "").strip()
    if not client_id:
        return jsonify({"error": "client_id is required."}), 400
 
    try:
        conn    = get_conn()
        uc_repo = PipelineConfigRepository(conn)
        cases   = uc_repo.list_for_client(client_id)
 
        # Return lightweight list (omit full tables_schema / column_mapping for speed)
        return jsonify({
            "client_id":  client_id,
            "use_cases": [
                {
                    "use_case_id":    uc["config_id"],
                    "use_case_name":  uc["use_case_name"],
                    "display_name":   uc["display_name"],
                    "golden_columns": len(uc.get("required_columns", [])),
                    "mapped_columns": len(uc.get("column_mapping", {})),
                    "tables":         [t["table_name"] for t in uc.get("tables_schema", [])],
                    "created_at":     uc["created_at"],
                }
                for uc in cases
            ],
        }), 200
 
    except Exception as e:
        err_str = str(e)
        # Table not created yet — return empty list instead of crashing
        if "use_case_definitions" in err_str and "does not exist" in err_str:
            return jsonify({"client_id": client_id, "use_cases": [], "_hint": "Run migration to create use_case_definitions table."}), 200
        import traceback; traceback.print_exc()
        return jsonify({"error": err_str}), 500
 
 
@bridge_bp.route("/use-cases/<use_case_name>", methods=["GET"])
def get_use_case(use_case_name):
    """
    Fetch full use case definition including column_mapping and tables_schema.
    Query param: client_id (required)
    """
    client_id = request.args.get("client_id", "").strip()
    if not client_id:
        return jsonify({"error": "client_id is required."}), 400
 
    try:
        conn    = get_conn()
        uc_repo = PipelineConfigRepository(conn)
        uc      = uc_repo.fetch_active(client_id, use_case_name)
        if not uc:
            return jsonify({"error": f"Use case '{use_case_name}' not found."}), 404
        return jsonify(uc), 200
 
    except Exception as e:
        return jsonify({"error": str(e)}), 500
 
 
@bridge_bp.route("/use-cases/<use_case_name>", methods=["DELETE"])
def delete_use_case(use_case_name):
    """Delete a use case. Query param: client_id"""
    client_id = request.args.get("client_id", "").strip()
    if not client_id:
        return jsonify({"error": "client_id is required."}), 400
    try:
        conn    = get_conn()
        uc_repo = PipelineConfigRepository(conn)
        deleted = uc_repo.delete(client_id, use_case_name)
        if not deleted:
            return jsonify({"error": "Use case not found."}), 404
        return jsonify({"message": f"Use case '{use_case_name}' deleted."}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
 
@bridge_bp.route("/use-cases/<use_case_name>/repair", methods=["POST"])
def repair_use_case(use_case_name):
    """
    Re-run _normalise_bundle on an existing use case to fix corrupted
    tables_schema / column_mapping (e.g. numeric ID garbage from bad export).
    The stored bundle is re-normalised and the row is updated in place.
 
    POST body (JSON): { "client_id": "..." }
    Optional: { "column_mapping": {"Trans_ID": "Transaction_ID", ...} }
              to manually supply a correct mapping.
    """
    import json as _json
    data      = request.get_json(force=True) or {}
    client_id = data.get("client_id", "").strip()
    if not client_id:
        return jsonify({"error": "client_id is required."}), 400
 
    manual_mapping = data.get("column_mapping")   # optional override
 
    try:
        conn    = get_conn()
        uc_repo = PipelineConfigRepository(conn)
        uc      = uc_repo.fetch_active(client_id, use_case_name)
        if not uc:
            return jsonify({"error": f"Use case '{use_case_name}' not found."}), 404

        bundle = {
            "use_case_name":   uc["use_case_name"],
            "tables":          uc.get("tables_schema", []),
            "column_mapping":  manual_mapping or uc.get("column_mapping", {}),
            "mapping_by_file": uc.get("mapping_by_file", {}),
            "source_client":   uc.get("source_client", ""),
            "exported_at":     uc.get("exported_at", ""),
        }
 
        tables_schema, column_mapping, required_columns, mapping_by_file = _normalise_bundle(bundle)
 
        # If manual mapping provided, use it directly
        if manual_mapping:
            column_mapping   = manual_mapping
            required_columns = list(set(required_columns + list(manual_mapping.values())))
 
        if not tables_schema:
            return jsonify({
                "error": (
                    f"Use case '{use_case_name}' has no tables schema and cannot be auto-repaired. "
                    "The original bundle had an empty 'tables' array. "
                    "Please re-export from bi_accelerator (which will fetch the live schema from "
                    "golden_schema_master) and re-import."
                ),
                "action": "re-export from bi_accelerator"
            }), 422
 
        uc_repo.upsert_definition(
            client_name      = client_id,
            use_case_name    = uc["use_case_name"],
            display_name     = uc.get("display_name"),
            tables_schema    = tables_schema,
            required_columns = required_columns,
            source_client    = uc.get("source_client"),
            exported_at      = uc.get("exported_at"),
            mapping_by_file  = mapping_by_file,
        )
 
        return jsonify({
            "message":        f"Use case '{use_case_name}' repaired.",
            "tables":         len(tables_schema),
            "golden_columns": len(required_columns),
            "mapped_columns": len(column_mapping),
            "required_columns_sample": required_columns[:5],
            "mapping_sample": dict(list(column_mapping.items())[:5]),
            "mapped_files":   list(mapping_by_file.keys()),
        }), 200
 
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500


# ── Power BI integration: read-only golden table endpoints ─────────────────
# Plain JSON, no auth (internal network only, per design decision). Power BI's
# "Web" data source connector polls these on a schedule; each call returns
# the full current contents of that golden table, JSON-safe and ordered by
# primary key for deterministic refreshes.

@bridge_bp.route("/golden/customers", methods=["GET"])
def get_golden_customers():
    """Power BI feed: golden_master_customer (Master_Customer dimension)."""
    try:
        conn = get_conn()
        repo = GoldenTableRepository(conn)
        rows = repo.fetch_customers()
        return jsonify({"table": "golden_master_customer", "row_count": len(rows), "data": rows}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bridge_bp.route("/golden/products", methods=["GET"])
def get_golden_products():
    """Power BI feed: golden_master_product (Master_Product dimension)."""
    try:
        conn = get_conn()
        repo = GoldenTableRepository(conn)
        rows = repo.fetch_products()
        return jsonify({"table": "golden_master_product", "row_count": len(rows), "data": rows}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bridge_bp.route("/golden/regions", methods=["GET"])
def get_golden_regions():
    """Power BI feed: golden_master_region (Master_Region dimension)."""
    try:
        conn = get_conn()
        repo = GoldenTableRepository(conn)
        rows = repo.fetch_regions()
        return jsonify({"table": "golden_master_region", "row_count": len(rows), "data": rows}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bridge_bp.route("/golden/salespersons", methods=["GET"])
def get_golden_salespersons():
    """Power BI feed: golden_master_salesperson (Master_Salesperson dimension)."""
    try:
        conn = get_conn()
        repo = GoldenTableRepository(conn)
        rows = repo.fetch_salespersons()
        return jsonify({"table": "golden_master_salesperson", "row_count": len(rows), "data": rows}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bridge_bp.route("/golden/transactions", methods=["GET"])
def get_golden_transactions():
    """Power BI feed: golden_sales_transaction (Sales_Transaction fact table)."""
    try:
        conn = get_conn()
        repo = GoldenTableRepository(conn)
        rows = repo.fetch_transactions()
        return jsonify({"table": "golden_sales_transaction", "row_count": len(rows), "data": rows}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bridge_bp.route('/golden/<table_name>', methods=['GET'])
def get_golden_data_for_powerbi(table_name):
    """
    Dynamic endpoint for Power BI.
    URL Format: /v1/golden/<table_name>?client_id=dk&use_case=sales
    """
    client_id = request.args.get('client_id', '').strip().lower()
    use_case  = request.args.get('use_case', '').strip().lower()
    
    if not client_id or not use_case:
        return jsonify({"error": "Missing required parameters: ?client_id=...&use_case=..."}), 400
        
    # Sanitize inputs to prevent SQL injection
    safe_client = re.sub(r'[^a-z0-9_]', '', client_id)
    safe_uc     = re.sub(r'[^a-z0-9_]', '', use_case)
    
    # Ensure the table name is safe. Automatically append 'golden_' if Power BI omits it.
    raw_table = table_name.lower()
    if not raw_table.startswith('golden_'):
        raw_table = f"golden_{raw_table}"
    safe_table = re.sub(r'[^a-z0-9_]', '', raw_table)

    try:
        data = []
        conn = get_conn()
        with conn.cursor() as cur:
            # ─── DYNAMIC QUERY WITH LINEAGE FILTERING ───
            # Since your Golden Tables use 'batch_id' rather than storing client_id natively,
            # we securely join back to the batch_master to ensure data isolation.
            query = f"""
                SELECT t.* FROM {safe_table} t
                JOIN batch_master b ON t.batch_id = b.batch_id
                WHERE b.client_id = %s 
                  AND (b.use_case_id = %s OR b.use_case_id IN (
                      SELECT use_case_id::text FROM use_case_definitions WHERE use_case_name = %s
                  ))
            """
            
            # Execute safely with parameterized inputs
            cur.execute(query, (safe_client, safe_uc, safe_uc))
            
            # Fetch column headers dynamically
            columns = [desc[0] for desc in cur.description]
            
            # Build the JSON response
            for row in cur.fetchall():
                row_dict = dict(zip(columns, row))
                for k, v in row_dict.items():
                    # Serialize dates and datetimes
                    if isinstance(v, (date, datetime)):
                        row_dict[k] = v.isoformat()
                    # Serialize UUIDs safely for JSON
                    elif hasattr(v, 'hex') or type(v).__name__ == 'UUID':
                        row_dict[k] = str(v)
                data.append(row_dict)

        return jsonify({
            "status": "success", 
            "table_name": safe_table,
            "row_count": len(data), 
            "data": data
        }), 200
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if 'conn' in locals() and not conn.closed:
            conn.close()