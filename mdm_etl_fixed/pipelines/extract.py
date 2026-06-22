"""
pipelines/extract.py
Accepts a CSV file object (werkzeug FileStorage or path string) and pushes
every row into unified_raw_staging.  Returns the row count so the
orchestrator can wire it into the report.
"""
import hashlib
import io

import pandas as pd

from core.database import get_conn
from database.repositories import BatchMasterRepository, UnifiedRawStagingRepository


def extract_data(
    batch_id: str,
    csv_file=None,       # werkzeug FileStorage  OR  str path  OR  bytes
    file_name: str = "upload.csv",
    dataset_type: str = "csv_upload",
    table_name: str = "csv_upload",
):
    """
    Extract data from a CSV upload and persist rows to unified_raw_staging.

    client_id/use_case_id removed from the signature — they were accepted
    previously but never actually used anywhere in this function body, so
    this cleanup is independent of the batch_master/batch_file_registry
    merge below.

    File info (dataset_type, file_name, file_hash) is now attached
    directly to the existing batch_master row instead of a separate
    batch_file_registry insert. batch_master is created earlier by the
    caller (before the file is read), so this is an UPDATE, not an INSERT
    — the file's hash genuinely isn't knowable until after the bytes are
    read here.

    Parameters
    ----------
    batch_id      : UUID string for the current batch (already created in
                    batch_master before this is called)
    csv_file      : werkzeug FileStorage, a file path str, or raw bytes
    file_name     : original file name
    dataset_type  : label stored in unified_raw_staging.dataset_type / batch_master.dataset_type
    table_name    : label stored in unified_raw_staging.table_name

    Returns
    -------
    dict  { "rows_extracted": int, "columns": list[str] }
    """
    if csv_file is None:
        return {"rows_extracted": 0, "columns": []}

    # ── Read CSV into DataFrame ────────────────────────────────────────────
    if isinstance(csv_file, (str,)):
        # Path string
        df = pd.read_csv(csv_file, dtype=str, keep_default_na=False)
        raw_bytes = open(csv_file, "rb").read()
    elif isinstance(csv_file, (bytes, bytearray)):
        raw_bytes = bytes(csv_file)
        df = pd.read_csv(io.BytesIO(raw_bytes), dtype=str, keep_default_na=False)
    else:
        # werkzeug FileStorage or file-like
        raw_bytes = csv_file.read()
        if hasattr(csv_file, "seek"):
            csv_file.seek(0)
        df = pd.read_csv(io.BytesIO(raw_bytes), dtype=str, keep_default_na=False)

    # Strip whitespace from column names
    df.columns = [c.strip() for c in df.columns]

    # ── Hash the file for deduplication tracking ───────────────────────────
    file_hash = hashlib.sha256(raw_bytes).hexdigest()

    conn         = get_conn()
    batch_repo   = BatchMasterRepository(conn)
    staging_repo = UnifiedRawStagingRepository(conn)

    # ── Attach file info to the already-existing batch_master row ─────────
    batch_repo.attach_file_info(
        batch_id=batch_id,
        dataset_type=dataset_type,
        file_name=file_name,
        file_hash=file_hash,
    )

    # ── Bulk insert rows into unified_raw_staging ─────────────────────────
    rows = df.to_dict(orient="records")
    staging_repo.bulk_insert(
        batch_id=batch_id,
        dataset_type=dataset_type,
        table_name=table_name,
        rows=rows,
    )

    batch_repo.update_file_status(batch_id, "EXTRACTED", rows_extracted=len(rows))

    return {
        "rows_extracted": len(rows),
        "columns": df.columns.tolist(),
    }