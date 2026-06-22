from core.database import get_conn
from database.repositories import StagingPromotionRepository, EtlRunLogRepository

# CHANGED: Added source_table to the arguments
def promote_data(batch_id: str, staging_table: str, prod_table: str, source_table: str, patch_missing_keys: bool = False):
    conn      = get_conn()
    promo_repo = StagingPromotionRepository(conn)
    log_repo   = EtlRunLogRepository(conn)

    try:
        stats = promo_repo.incremental_promote(
            staging_table=staging_table,
            prod_table=prod_table,
            source_table=source_table, # <--- NEW: Pass it down to the repository
            run_id=batch_id,
            patch_missing_keys=patch_missing_keys,
        )
        
        log_repo.insert(
            run_id=None,
            batch_id=batch_id,
            step_name="LOAD_PROMOTION",
            event_type="INFO",
            status="COMPLETED",
            detail=stats,
        )
        return stats

    except Exception as e:
        conn.rollback() 
        log_repo.insert(
            run_id=None,
            batch_id=batch_id,
            step_name="LOAD_PROMOTION",
            event_type="ERROR",
            status="FAILED",
            detail={"error": str(e)},
        )
        raise