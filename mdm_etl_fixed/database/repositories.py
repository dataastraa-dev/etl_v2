from __future__ import annotations
 
import hashlib
import json
import uuid
from datetime import datetime
from typing import Any, Optional
 
import psycopg2.extras
 
 
# ---------------------------------------------------------------------------
# HOW CONNECTIONS WORK HERE
# ---------------------------------------------------------------------------
# All repositories receive a raw psycopg2 connection via get_conn() from
# core/database.py — NOT a SQLAlchemy Connection object.
# ---------------------------------------------------------------------------
 
 
# ---------------------------------------------------------------------------
# 1. Config & Registry
# ---------------------------------------------------------------------------
 
class PipelineConfigRepository:
    """
    MERGED 2026-06: replaces three separate repositories —
    ConfigRepository (etl_pipeline_config), ClientPipelineConfigRepository
    (client_pipeline_config), and UseCaseRepository (use_case_definitions).
    All three tables are now one table, pipeline_config, keyed by
    (client_name, use_case_name) with config_id as the surrogate key every
    other table (batch_master, etc.) references instead of repeating the
    two name strings.

    Two upsert entry points correspond to the two workflows that used to
    hit two different tables and now both hit this one row:
      - upsert_config()      : the UV/UT rule-commit flow (was
                                ClientPipelineConfigRepository.create())
      - upsert_definition()  : the BI Accelerator import flow (was
                                UseCaseRepository.upsert())

    schema_id is nullable because upsert_definition() can run before
    upsert_config() ever has for a given client+use-case.
    """

    _JSON_FIELDS = (
        "tables_schema", "required_columns", "mapping_by_file",
        "column_mapping", "transformation_rules",
    )

    def __init__(self, conn) -> None:
        self._conn = conn

    # ── internal helpers ────────────────────────────────────────────────
    def _row_to_dict(self, cur, row) -> dict:
        cols = [d[0] for d in cur.description]
        result = dict(zip(cols, row))
        for field in self._JSON_FIELDS:
            if isinstance(result.get(field), str):
                try:
                    result[field] = json.loads(result[field])
                except Exception:
                    pass
        for field in ("config_id", "schema_id", "created_at"):
            if result.get(field) is not None:
                result[field] = str(result[field])
        return result

    # ── lookups ─────────────────────────────────────────────────────────
    def resolve_config_id(self, client_name: str, use_case_name: str) -> Optional[str]:
        sql = """
            SELECT config_id FROM pipeline_config
            WHERE client_name = %s AND use_case_name = %s AND is_active = TRUE
            ORDER BY config_id
            LIMIT 1
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, (client_name, use_case_name))
            row = cur.fetchone()
        return str(row[0]) if row else None

    def fetch_by_id(self, config_id: str) -> Optional[dict[str, Any]]:
        """
        Replaces ClientPipelineConfigRepository.fetch_active(config_id=...).
        Still LEFT JOINs golden_schema_registry for mandatory_columns /
        expected_data_types exactly as the original did — those have never
        been stored on the config row itself, just joined live at fetch time.
        """
        sql = """
            SELECT pc.*,
                   gsr.mandatory_columns, gsr.expected_data_types
            FROM pipeline_config pc
            LEFT JOIN golden_schema_registry gsr ON pc.schema_id = gsr.schema_id
            WHERE pc.config_id = %s
            LIMIT 1
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, (config_id,))
            row = cur.fetchone()
            if not row:
                return None
            result = self._row_to_dict(cur, row)
        if result.get("mandatory_columns") is None:
            result["mandatory_columns"] = []
        if result.get("expected_data_types") is None:
            result["expected_data_types"] = {}
        return result

    def fetch_active(self, client_name: str, use_case_name: str) -> Optional[dict[str, Any]]:
        """Replaces ClientPipelineConfigRepository.fetch_active(client_id, use_case_id)."""
        config_id = self.resolve_config_id(client_name, use_case_name)
        return self.fetch_by_id(config_id) if config_id else None

    def list_for_client(self, client_name: str) -> list[dict]:
        """Replaces UseCaseRepository.list_for_client()."""
        sql = """
            SELECT config_id, use_case_name, display_name,
                   tables_schema, column_mapping, required_columns,
                   mapping_by_file, source_client, exported_at, created_at
            FROM pipeline_config
            WHERE client_name = %s
            ORDER BY created_at DESC
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, (client_name,))
            cols = [d[0] for d in cur.description]
            results = [dict(zip(cols, row)) for row in cur.fetchall()]
        for r in results:
            for field in self._JSON_FIELDS:
                if isinstance(r.get(field), str):
                    try:
                        r[field] = json.loads(r[field])
                    except Exception:
                        pass
            for field in ("config_id", "created_at"):
                if r.get(field) is not None:
                    r[field] = str(r[field])
        return results

    # ── writes ──────────────────────────────────────────────────────────
    def ensure_exists(self, client_name: str, use_case_name: str) -> str:
        """
        Create a bare pipeline_config row with default control flags if one
        doesn't already exist. Replaces ConfigRepository.upsert_base_config().
        schema_id stays NULL until upsert_config() fills it in.
        """
        new_id = str(uuid.uuid4())
        sql = """
            INSERT INTO pipeline_config (
                config_id, client_name, use_case_name,
                stop_pipeline, is_quarantined, ingestion_strategy, is_active
            )
            VALUES (%s, %s, %s, FALSE, FALSE, 'OVERWRITE', TRUE)
            ON CONFLICT (client_name, use_case_name) DO NOTHING
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, (new_id, client_name, use_case_name))
        self._conn.commit()
        return self.resolve_config_id(client_name, use_case_name)

    def upsert_config(
        self,
        client_name: str,
        use_case_name: str,
        schema_id: str,
        column_mapping: dict,
        transformation_rules: list,
        patch_missing_keys: bool = False,
    ) -> str:
        """
        Replaces ClientPipelineConfigRepository.create(). Same self-healing
        behavior as the original: exactly one row per (client_name,
        use_case_name) — if duplicates already exist (legacy data), the
        lowest config_id is kept canonical and the rest deactivated, so
        existing batch_master.config_id FKs into the canonical row stay
        valid. Only touches operational columns — definition columns are
        left untouched if the row already exists.
        """
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT config_id FROM pipeline_config
                WHERE client_name = %s AND use_case_name = %s
                ORDER BY config_id
                """,
                (client_name, use_case_name),
            )
            existing_rows = [str(r[0]) for r in cur.fetchall()]

            if existing_rows:
                config_id = existing_rows[0]
                duplicates = existing_rows[1:]

                cur.execute(
                    """
                    UPDATE pipeline_config
                    SET schema_id            = %s,
                        column_mapping        = %s::jsonb,
                        transformation_rules  = %s::jsonb,
                        patch_missing_keys    = %s,
                        is_active             = TRUE
                    WHERE config_id = %s
                    """,
                    (
                        schema_id,
                        json.dumps(column_mapping),
                        json.dumps(transformation_rules),
                        bool(patch_missing_keys),
                        config_id,
                    ),
                )

                if duplicates:
                    cur.execute(
                        """
                        UPDATE pipeline_config
                        SET is_active = FALSE
                        WHERE config_id = ANY(%s::uuid[])
                        """,
                        (duplicates,),
                    )
            else:
                config_id = str(uuid.uuid4())
                cur.execute(
                    """
                    INSERT INTO pipeline_config
                        (config_id, client_name, use_case_name, schema_id,
                         column_mapping, transformation_rules, patch_missing_keys, is_active)
                    VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, TRUE)
                    """,
                    (
                        config_id,
                        client_name,
                        use_case_name,
                        schema_id,
                        json.dumps(column_mapping),
                        json.dumps(transformation_rules),
                        bool(patch_missing_keys),
                    ),
                )
        self._conn.commit()
        return config_id

    def upsert_definition(
        self,
        client_name: str,
        use_case_name: str,
        tables_schema: list,
        required_columns: list,
        display_name: Optional[str] = None,
        source_client: Optional[str] = None,
        exported_at: Optional[str] = None,
        mapping_by_file: Optional[dict] = None,
    ) -> str:
        """
        Replaces UseCaseRepository.upsert(). Can run before upsert_config()
        ever has for this client+use-case — creates a bare row (schema_id
        NULL, default control flags) if none exists yet. Same self-healing
        dedup as upsert_config so the two never fight over duplicate rows.
        Only touches definition columns — operational columns (schema_id,
        column_mapping, transformation_rules, patch_missing_keys) are left
        alone if the row already exists.
        """
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT config_id FROM pipeline_config
                WHERE client_name = %s AND use_case_name = %s
                ORDER BY config_id
                """,
                (client_name, use_case_name),
            )
            existing_rows = [str(r[0]) for r in cur.fetchall()]

            if existing_rows:
                config_id = existing_rows[0]
                duplicates = existing_rows[1:]
                cur.execute(
                    """
                    UPDATE pipeline_config
                    SET display_name      = %s,
                        tables_schema      = %s::jsonb,
                        required_columns   = %s::jsonb,
                        source_client      = %s,
                        exported_at        = %s,
                        mapping_by_file    = %s::jsonb
                    WHERE config_id = %s
                    """,
                    (
                        display_name or use_case_name,
                        json.dumps(tables_schema),
                        json.dumps(required_columns),
                        source_client,
                        exported_at,
                        json.dumps(mapping_by_file or {}),
                        config_id,
                    ),
                )
                if duplicates:
                    cur.execute(
                        """
                        UPDATE pipeline_config
                        SET is_active = FALSE
                        WHERE config_id = ANY(%s::uuid[])
                        """,
                        (duplicates,),
                    )
            else:
                config_id = str(uuid.uuid4())
                cur.execute(
                    """
                    INSERT INTO pipeline_config
                        (config_id, client_name, use_case_name, display_name,
                         tables_schema, required_columns, source_client,
                         exported_at, mapping_by_file,
                         stop_pipeline, is_quarantined, ingestion_strategy, is_active)
                    VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s::jsonb,
                            FALSE, FALSE, 'OVERWRITE', TRUE)
                    """,
                    (
                        config_id,
                        client_name,
                        use_case_name,
                        display_name or use_case_name,
                        json.dumps(tables_schema),
                        json.dumps(required_columns),
                        source_client,
                        exported_at,
                        json.dumps(mapping_by_file or {}),
                    ),
                )
        self._conn.commit()
        return config_id

    def delete(self, client_name: str, use_case_name: str) -> bool:
        """
        Replaces UseCaseRepository.delete(). BEHAVIOR CHANGE worth flagging:
        the original delete() only ever touched use_case_definitions and
        left client_pipeline_config alone. Since they're the same row now,
        this deletes the ENTIRE pipeline_config row — operational config
        included, not just definition metadata. If "clear the definition
        but keep the operational config" is needed as a distinct action,
        that needs a new method (UPDATE definition columns to NULL/defaults)
        rather than this one.
        """
        sql = "DELETE FROM pipeline_config WHERE client_name = %s AND use_case_name = %s"
        with self._conn.cursor() as cur:
            cur.execute(sql, (client_name, use_case_name))
            deleted = cur.rowcount > 0
        self._conn.commit()
        return deleted


class GoldenSchemaRegistryRepository:
    def __init__(self, conn) -> None:
        self._conn = conn
 
    def upsert(
        self,
        dataset_type: str,
        version: str,
        mandatory_columns: list[str],
        expected_data_types: dict[str, Any],
    ) -> str:
        sql = """
            INSERT INTO golden_schema_registry
                (dataset_type, version, mandatory_columns, expected_data_types)
            VALUES (%s, %s, %s::jsonb, %s::jsonb)
            ON CONFLICT (dataset_type, version)
            DO UPDATE SET
                mandatory_columns   = EXCLUDED.mandatory_columns,
                expected_data_types = EXCLUDED.expected_data_types
            RETURNING schema_id
        """
        with self._conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    dataset_type,
                    version,
                    json.dumps(mandatory_columns),
                    json.dumps(expected_data_types),
                ),
            )
            schema_id = cur.fetchone()[0]
        self._conn.commit()
        return str(schema_id)
 
    def fetch_latest(self, dataset_type: str) -> Optional[dict[str, Any]]:
        sql = """
            SELECT * FROM golden_schema_registry
            WHERE dataset_type = %s
            ORDER BY created_at DESC
            LIMIT 1
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, (dataset_type,))
            row = cur.fetchone()
            if not row:
                return None
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))


# ---------------------------------------------------------------------------
# 2. Batch & Orchestration
# ---------------------------------------------------------------------------

class BatchMasterRepository:
    """
    MERGED 2026-06: batch_file_registry folded in — a batch is always
    exactly one file, so file-level fields now live directly on this row
    instead of a separate table. client_id/use_case_id columns removed;
    resolve client/use-case via config_id -> pipeline_config instead.
    """

    def __init__(self, conn) -> None:
        self._conn = conn

    def create(
        self,
        run_by: str,
        config_id: Optional[str] = None,
        client_name: Optional[str] = None,
        use_case_name: Optional[str] = None,
    ) -> str:
        """
        client_name/use_case_name are accepted only as a fallback to resolve
        config_id when the caller doesn't have it yet — batch_master no
        longer stores them itself.

        (Dropped two unused params from the original signature,
        source_experiment_tag/source_client_name — neither was ever
        referenced in the INSERT below; unrelated dead code, not part of
        this merge.)
        """
        batch_id = str(uuid.uuid4())

        if not config_id:
            if not (client_name and use_case_name):
                raise ValueError(
                    "create() requires either config_id, or both client_name "
                    "and use_case_name to resolve one."
                )
            sql_lookup = """
                SELECT config_id FROM pipeline_config
                WHERE client_name = %s AND use_case_name = %s AND is_active = TRUE
                ORDER BY config_id
                LIMIT 1
            """
            with self._conn.cursor() as cur:
                cur.execute(sql_lookup, (client_name, use_case_name))
                row = cur.fetchone()
            if not row:
                raise ValueError(
                    f"No active config found for client_name={client_name!r}, "
                    f"use_case_name={use_case_name!r}. Commit a config first."
                )
            config_id = str(row[0])

        sql = """
            INSERT INTO batch_master
                (batch_id, config_id, status, run_by,
                 dataset_type, file_name, file_hash)
            VALUES (%s, %s, 'PENDING', %s, %s, %s, %s)
        """
        # dataset_type/file_name/file_hash are NOT NULL with no DB default
        # (see models.py BatchMaster). The real values aren't knowable until
        # extract.py reads the file's bytes, so seed placeholders here —
        # attach_file_info() overwrites them moments later in the same
        # request. Placeholders are namespaced with batch_id so two
        # concurrently-pending batches never collide on file_hash if a
        # caller queries before attach_file_info runs.
        with self._conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    batch_id, config_id, run_by,
                    "PENDING", "PENDING", f"PENDING-{batch_id}",
                ),
            )
        self._conn.commit()
        return batch_id

    def attach_file_info(
        self,
        batch_id: str,
        dataset_type: str,
        file_name: str,
        file_hash: str,
    ) -> None:
        """
        Replaces BatchFileRegistryRepository.register(). batch_master
        already exists by the time extract.py knows the file's hash (only
        computable after the file bytes are read) — this is an UPDATE on
        the existing row, not an insert into a separate table.
        """
        sql = """
            UPDATE batch_master
            SET dataset_type = %s, file_name = %s, file_hash = %s
            WHERE batch_id = %s
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, (dataset_type, file_name, file_hash, batch_id))
        self._conn.commit()

    def update_file_status(
        self,
        batch_id: str,
        status: str,
        rows_extracted: Optional[int] = None,
    ) -> None:
        """Replaces BatchFileRegistryRepository.update_status()."""
        if rows_extracted is not None:
            sql = """
                UPDATE batch_master
                SET file_status = %s, rows_extracted = %s
                WHERE batch_id = %s
            """
            params = (status, rows_extracted, batch_id)
        else:
            sql = "UPDATE batch_master SET file_status = %s WHERE batch_id = %s"
            params = (status, batch_id)
        with self._conn.cursor() as cur:
            cur.execute(sql, params)
        self._conn.commit()

    def update_status(
        self,
        batch_id: str,
        status: str,
        completed_datetime: Optional[datetime] = None,
    ) -> None:
        if completed_datetime:
            sql = """
                UPDATE batch_master
                SET status = %s, completed_datetime = %s
                WHERE batch_id = %s
            """
            params = (status, completed_datetime, batch_id)
        else:
            sql = "UPDATE batch_master SET status = %s WHERE batch_id = %s"
            params = (status, batch_id)
        with self._conn.cursor() as cur:
            cur.execute(sql, params)
        self._conn.commit()

    def fetch(self, batch_id: str) -> Optional[dict[str, Any]]:
        sql = "SELECT * FROM batch_master WHERE batch_id = %s"
        with self._conn.cursor() as cur:
            cur.execute(sql, (batch_id,))
            row = cur.fetchone()
            if not row:
                return None
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))

    def fetch_active_by_client(
        self, client_name: str, use_case_name: str
    ) -> list[dict[str, Any]]:
        sql = """
            SELECT bm.* FROM batch_master bm
            JOIN pipeline_config pc ON pc.config_id = bm.config_id
            WHERE pc.client_name = %s AND pc.use_case_name = %s
              AND bm.status NOT IN ('COMPLETED', 'FAILED', 'ABORTED')
            ORDER BY bm.created_datetime DESC
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, (client_name, use_case_name))
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


class EtlRunLogRepository:
    def __init__(self, conn) -> None:
        self._conn = conn
 
    def insert(
        self,
        run_id: Any,
        batch_id: str,
        step_name: str,
        event_type: str,
        status: str,
        detail: dict,
    ) -> None:
        sql = """
            INSERT INTO etl_run_log
                (batch_id, step_name, event_type, status, detail)
            VALUES (%s, %s, %s, %s, %s::jsonb)
        """
        with self._conn.cursor() as cur:
            cur.execute(
                sql,
                (batch_id, step_name, event_type, status, json.dumps(detail)),
            )
        self._conn.commit()
 
    def fetch_by_batch(self, batch_id: str) -> list[dict]:
        sql = "SELECT * FROM etl_run_log WHERE batch_id = %s ORDER BY logged_at ASC"
        with self._conn.cursor() as cur:
            cur.execute(sql, (batch_id,))
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
 
 
# ---------------------------------------------------------------------------
# 3. Extraction & Quality
# ---------------------------------------------------------------------------
 
class UnifiedRawStagingRepository:
    def __init__(self, conn) -> None:
        self._conn = conn

    def bulk_insert(
        self,
        batch_id: str,
        dataset_type: str,
        table_name: str,
        rows: list[dict],
    ) -> None:
        """
        file_id param removed 2026-06 — redundant once batch_file_registry
        merged into batch_master (1 file == 1 batch, file_id always equaled
        batch_id). unified_raw_staging.file_id column was dropped too.
        """
        if not rows:
            return
        sql = """
            INSERT INTO unified_raw_staging
                (batch_id, dataset_type, table_name, raw_payload, row_hash)
            VALUES %s
            ON CONFLICT DO NOTHING
        """
        data = []
        for r in rows:
            canonical = json.dumps(r, sort_keys=True, ensure_ascii=False, default=str)
            row_hash  = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
            data.append((batch_id, dataset_type, table_name, canonical, row_hash))

        with self._conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, data)
        self._conn.commit()

    def fetch_by_batch(
        self, batch_id: str, table_name: Optional[str] = None
    ) -> list[dict[str, Any]]:
        if table_name:
            sql = """
                SELECT * FROM unified_raw_staging
                WHERE batch_id = %s AND table_name = %s
                ORDER BY created_datetime ASC
            """
            params = (batch_id, table_name)
        else:
            sql = """
                SELECT * FROM unified_raw_staging
                WHERE batch_id = %s
                ORDER BY table_name, created_datetime ASC
            """
            params = (batch_id,)
        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            rows = []
            for row in cur.fetchall():
                record = dict(zip(cols, row))
                if isinstance(record.get("raw_payload"), str):
                    record["raw_payload"] = json.loads(record["raw_payload"])
                rows.append(record)
        return rows

    # bulk_update_transformed() removed 2026-06 — wrote to
    # unified_raw_staging.transformed_payload, confirmed dead: not called
    # from extract.py, transform.py, load.py, or orchestrator.py. The
    # column itself was dropped from the table in the same migration.

class UnifiedTransformedStagingRepository:
    def __init__(self, conn) -> None:
        self._conn = conn

    def bulk_insert(self, rows_to_insert: list[tuple[str, str, str, str]]) -> None:
        if not rows_to_insert:
            return
            
        sql = """
            INSERT INTO unified_transformed_staging
                (staging_id, batch_id, table_name, payload)
            VALUES %s
            ON CONFLICT (staging_id) DO UPDATE 
            SET payload = EXCLUDED.payload
        """
        with self._conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, rows_to_insert)
        self._conn.commit()

class ReviewQueueRepository:
    def __init__(self, conn) -> None:
        self._conn = conn
 
    def enqueue(
        self,
        staging_id: str,
        rule_id: str,
        flagged_row: dict,
        severity: str,
        recommended_action: Optional[str] = None,
    ) -> None:
        sql = """
            INSERT INTO review_queue
                (staging_id, rule_id, flagged_row, severity, status)
            VALUES (%s, %s, %s::jsonb, %s, 'PENDING_REVIEW')
        """
        with self._conn.cursor() as cur:
            cur.execute(
                sql,
                (staging_id, rule_id, json.dumps(flagged_row), severity),
            )
        self._conn.commit()
 
 
class StagingPromotionRepository:
    def __init__(self, conn) -> None:
        self._conn = conn

    def incremental_promote(
        self,
        staging_table: str,
        prod_table: str,
        source_table: str,
        run_id: str,
        patch_missing_keys: bool = False,
    ) -> dict[str, Any]:
        
        # ─── 1. Security Gate (Prefix Enforcment) ───
        if not (prod_table.startswith("golden_") or prod_table.startswith("unified_")):
            raise ValueError(
                f"Security Violation: Target table '{prod_table}' is not allowed. "
                "Target tables must start with 'golden_' or 'unified_'."
            )

        # ─── 2. Dynamically verify table existence in PostgreSQL ───
        table_check_sql = """
            SELECT 1 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
              AND table_name = %s
        """
        with self._conn.cursor() as cur:
            cur.execute(table_check_sql, (prod_table,))
            if not cur.fetchone():
                raise ValueError(f"Target table '{prod_table}' does not exist in the database.")

        # ─── 3. Dynamically fetch the Primary Key(s) from PostgreSQL ───
        pk_sql = """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tco
            JOIN information_schema.key_column_usage kcu 
              ON kcu.constraint_name = tco.constraint_name
              AND kcu.constraint_schema = tco.constraint_schema
            WHERE tco.constraint_type = 'PRIMARY KEY'
              AND kcu.table_name = %s
        """
        with self._conn.cursor() as cur:
            cur.execute(pk_sql, (prod_table,))
            pk_cols = [row[0] for row in cur.fetchall()]
            
        if not pk_cols:
            raise ValueError(f"Target table '{prod_table}' has no Primary Key defined in the database.")
            
        primary_key_clause = ", ".join(pk_cols)

        # ─── 4. Check if Primary Key exists in the Staging Payload ───
        # missing_pks: PK columns where NOT A SINGLE row in this batch has a
        # value (today's existing behavior: skip the whole table).
        # patched_rows: when patch_missing_keys=True, the specific
        # (staging_id, column) pairs where ONLY SOME rows are missing the PK
        # — these get a unique per-row placeholder instead of failing the
        # bulk INSERT on a NULL-into-PRIMARY-KEY violation.
        missing_pks: list[str] = []
        patched_rows: list[dict[str, Any]] = []

        with self._conn.cursor() as cur:
            for pk in pk_cols:
                pk_check_sql = f"""
                    SELECT 1 FROM {staging_table}
                    WHERE batch_id = %s AND (payload->>%s) IS NOT NULL AND (payload->>%s) != ''
                    LIMIT 1
                """
                cur.execute(pk_check_sql, (run_id, pk, pk))
                if not cur.fetchone():
                    missing_pks.append(pk)

        if missing_pks and not patch_missing_keys:
            # Original behavior preserved exactly: raise a safe, detectable
            # error string so orchestrator.py knows this was an intentional skip.
            raise ValueError(f"SKIPPED: Missing Primary Key(s) in payload: {missing_pks}")

        if missing_pks and patch_missing_keys:
            # Every row in the batch is missing this PK column entirely —
            # patching still applies (every row gets its own unique
            # placeholder below), but we record it explicitly since this is
            # the "whole table had no PK at all" case rather than a handful
            # of rows.
            with self._conn.cursor() as cur:
                for pk in missing_pks:
                    cur.execute(
                        f"""
                        SELECT staging_id FROM {staging_table}
                        WHERE batch_id = %s AND table_name = %s
                        """,
                        (run_id, source_table),
                    )
                    for (sid,) in cur.fetchall():
                        patched_rows.append({
                            "table": prod_table,
                            "column": pk,
                            "staging_id": str(sid),
                            "patched_value": f"PATCH-{sid}",
                            "reason": "Primary key missing for every row in this batch",
                        })

        # ─── NEW: 4b. Per-row PK gaps (some rows have it, some don't) ───
        # Only relevant for PK columns that DID have at least one populated
        # value above (those in missing_pks are already fully handled).
        # Without patching, leaving these NULL would fail the bulk INSERT
        # with a NOT-NULL-on-primary-key violation, taking out every row in
        # the table for this batch — not just the offending ones.
        if patch_missing_keys:
            partially_missing_pks = [pk for pk in pk_cols if pk not in missing_pks]
            with self._conn.cursor() as cur:
                for pk in partially_missing_pks:
                    cur.execute(
                        f"""
                        SELECT staging_id FROM {staging_table}
                        WHERE batch_id = %s AND table_name = %s
                          AND (payload->>%s IS NULL OR payload->>%s = '')
                        """,
                        (run_id, source_table, pk, pk),
                    )
                    for (sid,) in cur.fetchall():
                        patched_rows.append({
                            "table": prod_table,
                            "column": pk,
                            "staging_id": str(sid),
                            "patched_value": f"PATCH-{sid}",
                            "reason": "Primary key missing for this row",
                        })

        # ─── 5. Fetch column names AND their data types ───
        col_sql = """
            SELECT column_name, data_type 
            FROM information_schema.columns
            WHERE table_name = %s AND table_schema = 'public'
            ORDER BY ordinal_position
        """

        with self._conn.cursor() as cur:
            cur.execute(col_sql, (prod_table,))
            schema_cols = cur.fetchall()

        if not schema_cols:
            raise ValueError(f"Target table {prod_table!r} has no columns or does not exist.")

        col_list = ", ".join([c[0] for c in schema_cols])
        
        # Build type-safe extraction logic WITH SQL ALIASES
        json_extracts = []
        for col_name, data_type in schema_cols:
            if col_name == 'batch_id':
                json_extracts.append(f"batch_id AS {col_name}")
            elif col_name == 'loaded_at':
                json_extracts.append(f"NOW() AS {col_name}")
            elif patch_missing_keys and col_name in pk_cols:
                # Missing PK -> unique per-row placeholder via staging_id,
                # so two rows missing the same key never collide on conflict.
                json_extracts.append(
                    f"CASE WHEN NULLIF(payload->>'{col_name}', '') IS NULL "
                    f"THEN CAST('PATCH-' || staging_id::text AS {data_type}) "
                    f"ELSE CAST(payload->>'{col_name}' AS {data_type}) END AS {col_name}"
                )
            else:
                json_extracts.append(f"CAST(NULLIF(payload->>'{col_name}', '') AS {data_type}) AS {col_name}")

        extract_sql = ", ".join(json_extracts)
        
        # Build the update constraints (excluding PKs and batch metadata)
        update_set_cols = [
            c[0] for c in schema_cols 
            if c[0] not in pk_cols and c[0] not in ('batch_id', 'loaded_at')
        ]
        
        if update_set_cols:
            update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_set_cols)
            update_set += ", loaded_at = NOW()"
        else:
            update_set = "loaded_at = NOW()"

        # NEW: Wrap the selection in a DISTINCT ON subquery to deduplicate 
        # repeating dimension keys (e.g., CUST-001) before UPSERTING.
        # Replace the upsert_sql inside incremental_promote with this:
        upsert_sql = f"""
            INSERT INTO {prod_table} ({col_list})
            SELECT {col_list} FROM (
                SELECT DISTINCT ON ({primary_key_clause}) 
                    {extract_sql}
                FROM {staging_table}
                WHERE batch_id = %s AND table_name = %s
                ORDER BY {primary_key_clause}, loaded_at DESC
            ) AS deduplicated_payload
            ON CONFLICT ({primary_key_clause}) DO UPDATE SET {update_set}
        """
        
        with self._conn.cursor() as cur:
            cur.execute(upsert_sql, (run_id, source_table)) 
            row_count = cur.rowcount
            
        self._conn.commit()
        stats: dict[str, Any] = {"promoted": row_count}
        if patched_rows:
            stats["patched_rows"] = patched_rows
        return stats

# ---------------------------------------------------------------------------
# 4. Utilities
# ---------------------------------------------------------------------------
 
class AuditLogRepository:
    def __init__(self, conn) -> None:
        self._conn = conn
 
    def insert(
        self,
        run_id: Any,
        batch_id: str,
        step_name: str,
        detail: dict,
        event_type: str = "INFO",
    ) -> None:
        sql = """
            INSERT INTO etl_run_log
                (batch_id, step_name, event_type, status, detail)
            VALUES (%s, %s, %s, 'INFO', %s::jsonb)
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, (batch_id, step_name, event_type, json.dumps(detail)))
        self._conn.commit()

# ---------------------------------------------------------------------------
# Use Case Definitions
# ---------------------------------------------------------------------------
#
# UseCaseRepository removed 2026-06 — use_case_definitions merged into
# pipeline_config. Its upsert()/list_for_client()/fetch()/delete() are now
# PipelineConfigRepository.upsert_definition()/list_for_client()/
# fetch_active()/delete() respectively. Note _ensure_table()'s
# create-table-if-missing side effect also goes away — correctly, since
# use_case_definitions no longer exists as its own table.


class GoldenTableRepository:
    """
    Read-only access to the golden (production) tables for external BI tools
    (Power BI's Web connector). Each method returns ALL rows for that table,
    JSON-safe (UUIDs/dates/Decimals converted to plain str/float) and in a
    stable order (by primary key) so repeated refreshes are deterministic.
    """

    # table_name -> (sql_table, order_by_column)
    _TABLES = {
        "customer":    ("golden_master_customer",     "customer_id"),
        "product":     ("golden_master_product",      "product_id"),
        "region":      ("golden_master_region",       "region_id"),
        "salesperson": ("golden_master_salesperson",  "sales_rep_id"),
        "transaction": ("golden_sales_transaction",    "transaction_id"),
    }

    def __init__(self, conn) -> None:
        self._conn = conn

    @staticmethod
    def _to_json_safe(value):
        """Convert DB types psycopg2 returns (UUID, date, Decimal) to JSON-safe types."""
        import uuid as _uuid
        import datetime as _dt
        from decimal import Decimal as _Decimal

        if value is None:
            return None
        if isinstance(value, _uuid.UUID):
            return str(value)
        if isinstance(value, (_dt.date, _dt.datetime)):
            return value.isoformat()
        if isinstance(value, _Decimal):
            return float(value)
        return value

    def _fetch_all(self, table_key: str) -> list[dict]:
        if table_key not in self._TABLES:
            raise ValueError(f"Unknown golden table key: {table_key!r}")
        sql_table, order_col = self._TABLES[table_key]
        sql = f"SELECT * FROM {sql_table} ORDER BY {order_col}"
        with self._conn.cursor() as cur:
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
        return [
            {col: self._to_json_safe(val) for col, val in zip(cols, row)}
            for row in rows
        ]

    def fetch_customers(self) -> list[dict]:
        return self._fetch_all("customer")

    def fetch_products(self) -> list[dict]:
        return self._fetch_all("product")

    def fetch_regions(self) -> list[dict]:
        return self._fetch_all("region")

    def fetch_salespersons(self) -> list[dict]:
        return self._fetch_all("salesperson")

    def fetch_transactions(self) -> list[dict]:
        return self._fetch_all("transaction")