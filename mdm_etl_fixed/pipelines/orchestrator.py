"""
pipelines/orchestrator.py
Runs the full ETL pipeline:
    1. Extract   — CSV → unified_raw_staging
    2. Transform — global GV/GT + user UV/UT → anomaly + transformation report
    3. Load      — staging → golden tables (optional, if load config present)

Stores the complete pipeline report in etl_run_log so it can be retrieved
later by the report endpoint.
"""
from __future__ import annotations

import json
from datetime import datetime

from core.database import get_conn
from database.repositories import BatchMasterRepository, EtlRunLogRepository, PipelineConfigRepository
from pipelines.extract import extract_data
from pipelines.transform import run_transformations
from pipelines.load import promote_data


def run_pipeline(
    batch_id: str,
    config_id: str = None,
    client_id: str = None,
    use_case_id: str = None,
    csv_file=None,
    file_name: str = "upload.csv",
    dataset_type: str = "csv_upload",
    table_name: str = "csv_upload",
) -> dict:
    """
    Execute the full ETL pipeline for one batch run.

    config_id is the canonical identifier now that etl_pipeline_config +
    client_pipeline_config + use_case_definitions are merged into
    pipeline_config. client_id/use_case_id are accepted only as a fallback
    path to resolve config_id when the caller doesn't have it yet (e.g. a
    UI that only knows client+use-case at submit time) — they are not
    queried against repeatedly through the rest of the run.

    Returns the complete pipeline_report dict.  Also persists it to
    etl_run_log so the /report endpoint can later retrieve it.
    """
    conn        = get_conn()
    log_repo    = EtlRunLogRepository(conn)
    batch_repo  = BatchMasterRepository(conn)
    config_repo = PipelineConfigRepository(conn)

    # ── Resolve config_id once, up front ────────────────────────────────
    if not config_id:
        if not (client_id and use_case_id):
            raise ValueError(
                "run_pipeline requires either config_id, or both client_id "
                "and use_case_id to resolve one."
            )
        config_id = config_repo.resolve_config_id(client_id, use_case_id)
        if not config_id:
            raise ValueError(
                f"No pipeline_config found for client '{client_id}', use case '{use_case_id}'."
            )

    pipeline_config = config_repo.fetch_by_id(config_id) or {}

    # client_id/use_case_id are now sourced from the resolved pipeline_config
    # row (the single source of truth) rather than trusted as-passed-in.
    client_id     = pipeline_config.get("client_name", client_id)
    use_case_id   = pipeline_config.get("use_case_name", use_case_id)
    use_case_name = pipeline_config.get("use_case_name")

    pipeline_report: dict = {
        "batch_id":       batch_id,
        "client_id":      client_id,
        "use_case_id":    use_case_id,
        "config_id":      config_id,
        "use_case_name":  use_case_name,
        "started_at":     datetime.utcnow().isoformat(),
        "steps":          [],
        "extract":        {},
        "transform":      {},
        "load":           {},
        "anomalies":      [],
        "transformations": [],
        "severity_counts": {},
        "patches":        [],
        "error":          None,
    }

    try:
        batch_repo.update_status(batch_id, "RUNNING")

        # ── Step 1: Extraction ─────────────────────────────────────────────
        log_repo.insert(
            run_id=None, batch_id=batch_id,
            step_name="EXTRACT", event_type="INFO",
            status="STARTED", detail={},
        )

        extract_result = extract_data(
            batch_id=batch_id,
            csv_file=csv_file,
            file_name=file_name,
            dataset_type=dataset_type,
            table_name=table_name,
        )
        extract_result["file_name"] = file_name   # ensure it's always present
        pipeline_report["extract"] = extract_result
        pipeline_report["steps"].append({
            "name":           "extract",
            "status":         "COMPLETED",
            "rows_extracted": extract_result.get("rows_extracted", 0),
            "file_name":      file_name,
        })

        log_repo.insert(
            run_id=None, batch_id=batch_id,
            step_name="EXTRACT", event_type="INFO",
            status="COMPLETED", detail=extract_result,
        )

        # ── Step 2: Transform & Validate ───────────────────────────────────
        log_repo.insert(
            run_id=None, batch_id=batch_id,
            step_name="TRANSFORM", event_type="INFO",
            status="STARTED", detail={},
        )

        transform_report = run_transformations(
            batch_id=batch_id,
            config_id=config_id,
            file_name=file_name,
        )

        pipeline_report["transform"]       = transform_report
        pipeline_report["anomalies"]       = transform_report.get("anomalies", [])
        pipeline_report["transformations"] = transform_report.get("transformations", [])
        pipeline_report["severity_counts"] = transform_report.get("severity_counts", {})

        pipeline_report["steps"].append({
            "name":            "transform",
            "status":          "COMPLETED",
            "rows_processed":  transform_report.get("rows_processed", 0),
            "anomaly_count":   len(pipeline_report["anomalies"]),
            "severity_counts": pipeline_report["severity_counts"],
        })

        log_repo.insert(
            run_id=None, batch_id=batch_id,
            step_name="TRANSFORM", event_type="INFO",
            status="COMPLETED",
            detail={
                "rows_processed":  transform_report.get("rows_processed", 0),
                "anomaly_count":   len(pipeline_report["anomalies"]),
                "severity_counts": pipeline_report["severity_counts"],
            },
        )

        # ── Step 3: Load (The Gate) ────────────────────────────────────────
        # RELAXED GATE: Only block the pipeline if there are CRITICAL anomalies.
        blocking_errors = pipeline_report["severity_counts"].get("CRITICAL", None)

        print(f"blocking_errors: {blocking_errors}", flush=True)
        if not blocking_errors:
            log_repo.insert(
                run_id=None, batch_id=batch_id,
                step_name="LOAD", event_type="INFO",
                status="STARTED", detail={},
            )
            print(f"[LOAD GATE] ✅ Pipeline passed with {blocking_errors} critical anomalies.", flush=True)
            
            try:
                # ── NEW: Bulletproof Multi-Table Schema Fan-Out ──────────────────
                target_tables = []

                # patch_missing_keys and tables_schema both now live on the
                # same pipeline_config row we already fetched up front —
                # no second/third query against separate tables needed.
                patch_missing_keys = bool(pipeline_config.get("patch_missing_keys", False))
                tables_schema = pipeline_config.get("tables_schema") or []
                if isinstance(tables_schema, str):
                    tables_schema = json.loads(tables_schema)

                for table_meta in tables_schema:
                    t_name = table_meta.get("table_name", "")
                    if t_name:
                        target_tables.append(f"golden_{t_name.lower()}")
                
                # If STILL no tables found, fallback to transaction table
                if not target_tables:
                    target_tables = [table_name if table_name != "csv_upload" else "golden_sales_transaction"]

                promoted_stats = {}
                all_patched_rows = []
                print(f"\\n[LOAD GATE PASSED] Target Tables resolved: {target_tables}", flush=True)
                
                # Loop through and populate every structural dimension + fact table
                for target_prod_table in target_tables:
                    print(f"  -> Attempting UPSERT for {target_prod_table}...", flush=True)
                    try:
                        table_stats = promote_data(
                            batch_id=batch_id, 
                            staging_table="unified_transformed_staging", 
                            prod_table=target_prod_table,
                            source_table=table_name,
                            patch_missing_keys=patch_missing_keys,
                        )
                        table_patched = (table_stats or {}).get("patched_rows") or []
                        if table_patched:
                            all_patched_rows.extend(table_patched)
                            promoted_stats[target_prod_table] = f"SUCCESS (patched {len(table_patched)} row(s) — see Patches Applied)"
                            print(f"  ✓ {target_prod_table} populated successfully, with {len(table_patched)} patched row(s).", flush=True)
                        else:
                            promoted_stats[target_prod_table] = "SUCCESS"
                            print(f"  ✓ {target_prod_table} populated successfully.", flush=True)
                    except Exception as table_exc:
                        err_msg = str(table_exc)
                        if "SKIPPED" in err_msg:
                            promoted_stats[target_prod_table] = err_msg
                            print(f"  ⏭ SKIPPED {target_prod_table} (Missing keys).", flush=True)
                        else:
                            promoted_stats[target_prod_table] = f"FAILED: {err_msg}"
                            print(f"  ❌ FAILED {target_prod_table}: {err_msg}", flush=True)
                
                pipeline_report["load"] = {"status": "COMPLETED", "details": promoted_stats}
                if all_patched_rows:
                    pipeline_report["patches"] = all_patched_rows
                pipeline_report["steps"].append({
                    "name": "load", 
                    "status": "COMPLETED",
                    "details": promoted_stats
                })
                
            except Exception as exc:
                pipeline_report["steps"].append({
                    "name": "load", 
                    "status": "FAILED", 
                    "error": str(exc)
                })
                pipeline_report["error"] = f"Load phase failed: {str(exc)}"
        else:
            pipeline_report["steps"].append({
                "name": "load",
                "status": "SKIPPED",
                "detail": f"Promotion blocked due to {blocking_errors} critical anomalies."
            })

        # ── Finalise Batch ─────────────────────────────────────────────────
        batch_repo.update_status(
            batch_id, "COMPLETED" if not pipeline_report.get("error") else "FAILED",
            completed_datetime=datetime.utcnow(),
        )
        pipeline_report["completed_at"] = datetime.utcnow().isoformat()

    except Exception as exc:
        # CLEAR ABORTED TRANSACTION BEFORE UPDATING STATUS
        conn.rollback()
        
        pipeline_report["error"] = str(exc)
        pipeline_report["steps"].append({"name": "pipeline", "status": "FAILED", "error": str(exc)})
        try:
            batch_repo.update_status(batch_id, "FAILED")
        except Exception:
            pass

    finally:
        # ── Always persist the full report to etl_run_log ─────────────────
        safe_report = _safe_json(pipeline_report)
        try:
            # CLEAR ANY LINGERING TRANSACTION STATES BEFORE FINAL LOG
            conn.rollback()
            
            log_repo.insert(
                run_id=None,
                batch_id=batch_id,
                step_name="PIPELINE_COMPLETE",
                event_type="REPORT",
                status="FINISHED" if not pipeline_report.get("error") else "FAILED",
                detail=safe_report,
            )
        except Exception:
            pass  # logging failure must not propagate

    return pipeline_report


# ── Helpers ───────────────────────────────────────────────────────────────

def _safe_json(obj):
    """Recursively convert non-serialisable types to str."""
    return json.loads(json.dumps(obj, default=str))