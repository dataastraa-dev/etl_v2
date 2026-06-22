"""
pipelines/transform.py
Full pipeline:
    Phase A  — Global Transformations (GT) on ALL data
    Phase A' — Global Validations  (GV) on ALL data
    Phase B  — User-defined Transformations (UT) only on columns listed in config
    Phase B' — User-defined Validations    (UV) only on columns listed in config
 
Returns a rich report dict consumed by orchestrator and report generator.
"""
from __future__ import annotations
 
import pandas as pd
import json
import numpy as np
from core.database import get_conn
from core.registry import StrategyRegistry
from database.repositories import (
    PipelineConfigRepository,
    ReviewQueueRepository,
    UnifiedRawStagingRepository,
    UnifiedTransformedStagingRepository,
)
 
# ── Ordered global transformation chain (Phase A) ─────────────────────────
GLOBAL_TRANSFORMERS = [
    "column_mapper",
    "strip_currency_symbols",
    "normalize_whitespace",
    "standardize_boolean",
    "enforce_date_format",
    "compute_derived_columns",   # must be last — depends on renamed/cleaned cols
]
 
# ── Global validation chain (Phase A') ────────────────────────────────────
# These names ARE registered in strategies/validations.py.
GLOBAL_VALIDATORS = [
    "required_columns_present",
    "no_duplicate_primary_keys",
    "non_negative_quantities",
    "no_future_transaction_dates",
]

# Minimum similarity score (0.0-1.0) required before a fuzzy filename match
# is trusted. Below this, we'd rather fall back to the flat column_mapping
# than silently apply the wrong file's mapping to this upload.
_FUZZY_FILENAME_THRESHOLD = 0.55


def _strip_hotfolder_prefix(name: str, client_id: str = None, use_case_name: str = None) -> str:
    """
    The hot folder convention is "clientID__useCaseName__timestamp.csv". That
    prefix is structurally known (not something to fuzzy-guess), so strip it
    off before comparing against the bundle's recorded filenames — otherwise
    "default_client__sales_dk__20260617094416.csv" gets compared in full
    against "DK_SampleData_Apr_to_Sep.csv" and the real filename signal gets
    drowned out by the client/use-case prefix, scoring far below threshold.

    Falls back to generic "strip anything that looks like client__usecase__"
    double-underscore segments if client_id/use_case_name aren't passed in,
    so this still helps even when called without them.
    """
    import re as _re
    remainder = name

    if client_id and use_case_name:
        prefix = f"{client_id}__{use_case_name}__"
        if remainder.lower().startswith(prefix.lower()):
            remainder = remainder[len(prefix):]

    # Generic fallback: strip up to 2 leading "segment__" chunks that look
    # like identifiers (no spaces, reasonably short) rather than part of
    # the real dataset name — covers the case where exact client_id/
    # use_case_name weren't available to this function.
    if remainder == name:
        parts = remainder.split("__")
        while len(parts) > 1 and len(parts[0]) <= 40 and " " not in parts[0]:
            parts.pop(0)
            if len(parts) <= 1:
                break
        remainder = "__".join(parts) if parts else remainder

    return remainder or name


def _normalise_filename_for_match(name: str) -> str:
    """
    Strip extension, lowercase, and remove common date/timestamp/version
    suffixes so "DK_SampleData_Apr_to_Sep_20260617.csv" still matches
    against a recorded "DK_SampleData_Apr_to_Sep.csv" mapping key.
    """
    import re as _re
    base = name.rsplit(".", 1)[0].lower().strip()
    # Strip trailing run-id/date-ish tokens like _20260617, _v2, -final, (1)
    base = _re.sub(r"[\s_\-]*\(?\d{4,}\)?$", "", base)
    base = _re.sub(r"[\s_\-]*v\d+$", "", base)
    base = _re.sub(r"[\s_\-]*final$", "", base)
    base = _re.sub(r"[^a-z0-9]+", "", base)  # collapse all separators for comparison
    return base


def _fuzzy_match_filename(
    uploaded_name: str,
    mapping_by_file: dict,
    client_id: str = None,
    use_case_name: str = None,
) -> tuple[str, dict, float]:
    """
    Find the best match for `uploaded_name` among mapping_by_file's keys.

    Returns (matched_filename, matched_mapping, score). If no candidate
    clears _FUZZY_FILENAME_THRESHOLD, matched_filename/mapping are "" / {}
    so the caller can fall back to the flat column_mapping rather than
    trust a low-confidence guess.
    """
    from difflib import SequenceMatcher

    stripped = _strip_hotfolder_prefix(uploaded_name, client_id, use_case_name)
    target   = _normalise_filename_for_match(stripped)
    if not target:
        return "", {}, 0.0

    best_name, best_map, best_score = "", {}, 0.0
    for candidate_name, candidate_map in mapping_by_file.items():
        cand_norm = _normalise_filename_for_match(candidate_name)
        if not cand_norm:
            continue
        score = SequenceMatcher(None, target, cand_norm).ratio()
        # Exact match (post-normalisation) always wins outright
        if target == cand_norm:
            return candidate_name, candidate_map, 1.0
        if score > best_score:
            best_name, best_map, best_score = candidate_name, candidate_map, score

    if best_score >= _FUZZY_FILENAME_THRESHOLD:
        return best_name, best_map, best_score
    return "", {}, best_score
 
 
def run_transformations(
    batch_id: str,
    config_id: str,
    file_name: str = None,
) -> dict:
    """
    Execute the full ETL transform pipeline for the given batch.

    config_id is the only identifier needed now — pipeline_config (the
    merge of etl_pipeline_config + client_pipeline_config +
    use_case_definitions) carries client_name, use_case_name,
    column_mapping, mapping_by_file, and transformation_rules all on the
    same row, fetched once below.
    """
    conn         = get_conn()
    config_repo  = PipelineConfigRepository(conn)
    review_repo  = ReviewQueueRepository(conn)
    staging_repo = UnifiedRawStagingRepository(conn)

    config        = config_repo.fetch_by_id(config_id) or {}
    raw_rules     = config.get("transformation_rules", [])
    client_id     = config.get("client_name")
    use_case_name = config.get("use_case_name")
 
    # ── DIAGNOSTIC: log what column_mapping is stored in pipeline_config
    _raw_cm_diag = config.get("column_mapping") or {}
    _first_val = list(_raw_cm_diag.values())[0] if _raw_cm_diag else None
    print(f"[DIAG] column_mapping keys in config: {list(_raw_cm_diag.keys())[:6]}", flush=True)
    print(f"[DIAG] first value type: {type(_first_val).__name__}, value[:100]: {str(_first_val)[:100]}", flush=True)
 
    # ── Resolve per-file mapping: if the use case was exported from
    # bi_accelerator with mapping_by_file (per-source-filename mappings,
    # since one use case can be fed by multiple files with different
    # columns), fuzzy-match the actual uploaded file_name against the
    # recorded filenames and use that file's specific mapping instead of
    # the generic flat column_mapping. This lets a single use case have
    # different files map differently without anything being merged or
    # guessed about row alignment — each file's own mapping is used for
    # its own batch.
    #
    # mapping_by_file now lives directly on this same config row (merged
    # in from use_case_definitions) — no second repository call needed.
    if file_name and use_case_name:
        mapping_by_file = config.get("mapping_by_file") or {}
        if mapping_by_file:
            matched_file, matched_mapping, score = _fuzzy_match_filename(
                file_name, mapping_by_file, client_id=client_id, use_case_name=use_case_name
            )
            print(f"[DIAG] fuzzy filename match: {file_name!r} -> {matched_file!r} (score={score:.2f})", flush=True)
            if matched_mapping:
                config = {**config, "column_mapping": matched_mapping}
                # Scope required_columns_present to ONLY the golden columns
                # THIS file's mapping actually targets. Without this, a
                # lookup/master file (e.g. SalesRep_Master.csv, which only
                # ever supplies sales_rep_id/sales_rep_name/manager_name/
                # busness_head) would be checked against the full mandatory
                # list for the whole use case — including columns that live
                # in completely different source files — and flag dozens of
                # "missing" columns that were never supposed to come from
                # this file in the first place.
                scoped_mandatory = sorted(set(matched_mapping.values()))
                config["mandatory_columns"] = scoped_mandatory
                print(f"[DIAG] scoped mandatory_columns for {matched_file!r} "
                      f"to {len(scoped_mandatory)} columns: {scoped_mandatory}", flush=True)
            else:
                print(f"[DIAG] No confident filename match for {file_name!r} among "
                      f"{list(mapping_by_file.keys())} — falling back to flat column_mapping "
                      f"and the FULL mandatory_columns list (may produce missing-column warnings "
                      f"if this file only covers part of the golden schema).", flush=True)
 
    # ── Pull all rows for this batch from staging ──────────────────────────
    raw_rows = staging_repo.fetch_by_batch(batch_id)
    if not raw_rows:
        return _empty_report()
 
    df          = pd.DataFrame([r["raw_payload"] for r in raw_rows])
    staging_ids = [r["staging_id"] for r in raw_rows]
 
    source_table_name = raw_rows[0].get("table_name", "") if raw_rows else ""
 
    # ── Normalise column_mapping ───────────────────────────────────────────
    raw_cm = config.get("column_mapping") or {}
    if raw_cm and any(isinstance(v, dict) for v in raw_cm.values()):
        flat_cm = (
            raw_cm.get(source_table_name)
            or raw_cm.get("csv_upload")
            or raw_cm.get("")
            or {}
        )
        if not flat_cm:
            flat_cm = {}
            for sub in raw_cm.values():
                if isinstance(sub, dict):
                    flat_cm.update(sub)
        config = {**config, "column_mapping": flat_cm}
 
    # ── DIAGNOSTIC: log what column_mapping looks like after flattening
    _cm_after = config.get("column_mapping") or {}
    print(f"[DIAG] source_table_name: {source_table_name!r}", flush=True)
    print(f"[DIAG] column_mapping after flatten — keys: {list(_cm_after.keys())[:6]}", flush=True)
    print(f"[DIAG] 'CUSTOMER_TRX_ID' in mapping: {'CUSTOMER_TRX_ID' in _cm_after}", flush=True)
    print(f"[DIAG] mapping sample: {dict(list(_cm_after.items())[:4])}", flush=True)
 
    if source_table_name and "production_table_name" not in config:
        config = {**config, "production_table_name": source_table_name}
 
    report = {
        "anomalies":        [],
        "transformations":  [],
        "rows_processed":   len(df),
        "columns_in_data":  df.columns.tolist(),
        "user_columns":     [],
        "phases": {
            "global_transform": [],
            "global_validate":  [],
            "user_transform":   [],
            "user_validate":    [],
        },
    }
 
    # ══════════════════════════════════════════════════════════════════════
    # PHASE A  — Global Transformations on ALL rows
    # ══════════════════════════════════════════════════════════════════════
    for strategy_name in GLOBAL_TRANSFORMERS:
        strategy = StrategyRegistry.get(strategy_name)
        if strategy is None:
            continue
        try:
            df_before_cols = set(df.columns)
            df, anomalies = strategy().transform(df, config)
            df_after_cols  = set(df.columns)
 
            cols_renamed = list(df_after_cols - df_before_cols)
            phase_entry = {
                "strategy":      strategy_name,
                "anomaly_count": len(anomalies),
                "cols_renamed":  cols_renamed,
            }
            report["phases"]["global_transform"].append(phase_entry)
            report["anomalies"].extend(anomalies)
 
            report["transformations"].append({
                "phase":        "global_transform",
                "strategy":     strategy_name,
                "severity":     "INFO",
                "message":      f"Transformer '{strategy_name}' applied successfully to {len(df)} rows.",
                "cols_renamed": cols_renamed,
                "anomaly_count": len(anomalies),
            })
            for a in anomalies:
                report["transformations"].append({
                    "phase":    "global_transform",
                    "strategy": strategy_name,
                    **a,
                })
        except Exception as exc:
            report["anomalies"].append({
                "rule":     strategy_name,
                "severity": "ERROR",
                "message":  f"Global transformer '{strategy_name}' raised: {exc}",
            })
 
    # ══════════════════════════════════════════════════════════════════════
    # PHASE A' — Global Validations on ALL rows
    # ══════════════════════════════════════════════════════════════════════
    for strategy_name in GLOBAL_VALIDATORS:
        strategy = StrategyRegistry.get(strategy_name)
        if strategy is None:
            continue
        try:
            anomalies = strategy().validate(df, config)
            report["phases"]["global_validate"].append({
                "strategy":      strategy_name,
                "anomaly_count": len(anomalies),
            })
            report["anomalies"].extend(anomalies)
        except Exception as exc:
            report["anomalies"].append({
                "rule":     strategy_name,
                "severity": "ERROR",
                "message":  f"Global validator '{strategy_name}' raised: {exc}",
            })

    # ══════════════════════════════════════════════════════════════════════
    # UI TO GOLDEN SCHEMA ADAPTER
    # ══════════════════════════════════════════════════════════════════════
    # Translates raw CSV column names in UI rules to mapped Golden Column names
    mapping = config.get("column_mapping", {})
    for rule in raw_rules:
        # 1. Translate the target column
        orig_col = rule.get("column", "")
        if orig_col in mapping:
            rule["column"] = mapping[orig_col]

        # 2. Translate parameter columns
        params = rule.get("parameters", {})
        
        # Translate 'source_columns' for rules like UT-5 (Concatenate)
        if "source_columns" in params:
            src = params["source_columns"]
            if isinstance(src, str):
                src_list = [c.strip() for c in src.split(",")]
                params["source_columns"] = ",".join([mapping.get(c, c) for c in src_list])
            elif isinstance(src, list):
                params["source_columns"] = [mapping.get(c, c) for c in src]
        
        # Translate 'condition_column' for rules like UT-8 (Conditional Fill)
        if "condition_column" in params:
            cond_col = params["condition_column"]
            if cond_col in mapping:
                params["condition_column"] = mapping[cond_col]
 
    # ══════════════════════════════════════════════════════════════════════
    # PHASE B  — User-defined Transformations on SELECTED columns only
    # ══════════════════════════════════════════════════════════════════════
    ut_rules = [r for r in raw_rules if r.get("type", "").upper() == "UT"]
    user_cols_touched = set()
 
    for rule in ut_rules:
        col           = rule.get("column")
        strategy_name = rule.get("rule") or rule.get("type_key")
        strategy      = StrategyRegistry.get(strategy_name)
        if strategy is None or col is None:
            continue
 
        user_cols_touched.add(col)
 
        rule_config = {**config, **rule}
        try:
            df, anomalies = strategy().transform(df, rule_config)
            report["phases"]["user_transform"].append({
                "strategy": strategy_name,
                "column":   col,
                "anomaly_count": len(anomalies),
            })
            report["transformations"].append({
                "phase":    "user_transform",
                "strategy": strategy_name,
                "column":   col,
                "severity": "INFO",
                "message":  (
                    f"Transformer '{strategy_name}' applied to column '{col}' "
                    f"on {len(df)} rows."
                ),
            })
            for a in anomalies:
                report["transformations"].append({
                    "phase":    "user_transform",
                    "strategy": strategy_name,
                    "column":   col,
                    **a,
                })
            report["anomalies"].extend(anomalies)
        except Exception as exc:
            report["anomalies"].append({
                "rule":     strategy_name,
                "column":   col,
                "severity": "ERROR",
                "message":  f"User transformer '{strategy_name}' on '{col}' raised: {exc}",
            })
 
    # ══════════════════════════════════════════════════════════════════════
    # PHASE B' — User-defined Validations on SELECTED columns only
    # ══════════════════════════════════════════════════════════════════════
    uv_rules = [r for r in raw_rules if r.get("type", "").upper() == "UV"]
 
    for rule in uv_rules:
        col           = rule.get("column")
        strategy_name = rule.get("rule") or rule.get("type_key")
        strategy      = StrategyRegistry.get(strategy_name)
        if strategy is None or col is None:
            continue
 
        user_cols_touched.add(col)
        rule_config = {**config, **rule}
 
        try:
            anomalies = strategy().validate(df, rule_config)
            report["phases"]["user_validate"].append({
                "strategy":      strategy_name,
                "column":        col,
                "anomaly_count": len(anomalies),
            })
 
            for ann in anomalies:
                if ann.get("severity") == "FLAGGED":
                    affected = ann.get("affected_rows")
                    if affected:
                        row_indices = list(affected)
                    elif ann.get("row_index") is not None:
                        row_indices = [ann["row_index"]]
                    else:
                        row_indices = [None]
 
                    for row_idx in row_indices:
                        staging_id = (
                            staging_ids[row_idx]
                            if row_idx is not None and row_idx < len(staging_ids)
                            else None
                        )
 
                        if row_idx is not None and row_idx < len(df):
                            row_series = df.iloc[row_idx].replace({np.nan: None})
                            row_data = row_series.to_dict()
                        else:
                            row_data = {}
 
                        try:
                            review_repo.enqueue(
                                staging_id=staging_id,
                                rule_id=rule.get("rule_id", strategy_name),
                                flagged_row=row_data,
                                severity=ann["severity"],
                                recommended_action=ann.get("recommended_action"),
                            )
                        except Exception:
                            pass  
 
                report["anomalies"].append(ann)
 
        except Exception as exc:
            report["anomalies"].append({
                "rule":     strategy_name,
                "column":   col,
                "severity": "ERROR",
                "message":  f"User validator '{strategy_name}' on '{col}' raised: {exc}",
            })
 
    report["user_columns"] = sorted(user_cols_touched)
 
    # ── Severity summary ───────────────────────────────────────────────────
    report["severity_counts"] = _count_severities(report["anomalies"])
 
    # ══════════════════════════════════════════════════════════════════════
    # FINAL PHASE — Medallion Write-Out (Bronze -> Silver)
    # ══════════════════════════════════════════════════════════════════════
    if not df.empty:
        transformed_repo = UnifiedTransformedStagingRepository(get_conn())
        
        current_table_name = raw_rows[0].get("table_name", "csv_upload") if raw_rows else "csv_upload"
 
        df_clean = df.replace({np.nan: None})
        transformed_records = df_clean.to_dict(orient="records")
        
        insert_data = [
            (str(sid), batch_id, current_table_name, json.dumps(record, default=str))
            for sid, record in zip(staging_ids, transformed_records)
        ]
        
        try:
            transformed_repo.bulk_insert(insert_data)
        except Exception as exc:
            report["anomalies"].append({
                "rule": "silver_layer_write",
                "severity": "ERROR",
                "message": f"Failed to write transformed data to Silver layer: {exc}",
            })
            report["severity_counts"] = _count_severities(report["anomalies"])
 
    return report
 
 
# ── Helpers ───────────────────────────────────────────────────────────────
 
def _empty_report() -> dict:
    return {
        "anomalies":       [],
        "transformations": [],
        "rows_processed":  0,
        "columns_in_data": [],
        "user_columns":    [],
        "severity_counts": {},
        "phases": {
            "global_transform": [],
            "global_validate":  [],
            "user_transform":   [],
            "user_validate":    [],
        },
    }
 
 
def _count_severities(anomalies: list[dict]) -> dict:
    counts: dict[str, int] = {}
    for a in anomalies:
        sev = a.get("severity", "UNKNOWN")
        counts[sev] = counts.get(sev, 0) + 1
    return counts