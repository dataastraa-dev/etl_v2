import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import (
    Boolean, CheckConstraint, Column, Date, ForeignKey, 
    ForeignKeyConstraint, Index, Integer, Numeric, String, Text, UniqueConstraint, text
)
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass

# ===========================================================================
# 1. CONFIGURATION & MAPPING LAYER
# ===========================================================================

class GoldenSchemaRegistry(Base):
    __tablename__ = "golden_schema_registry"

    schema_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    dataset_type: Mapped[str] = mapped_column(String(100), nullable=False)
    version: Mapped[str] = mapped_column(String(50), nullable=False)
    mandatory_columns: Mapped[list] = mapped_column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))
    expected_data_types: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text("NOW()"))

    __table_args__ = (UniqueConstraint("dataset_type", "version", name="uq_golden_schema_dataset_version"),)


class PipelineConfig(Base):
    """
    MERGED 2026-06: formerly three tables — etl_pipeline_config,
    client_pipeline_config, and use_case_definitions.

    client_name / use_case_name now live ONLY here. Every other table
    that needs "which client + which use case" references this table's
    config_id instead of repeating the two strings.

    config_id is generated once per (client_name, use_case_name) — see
    uq_pipeline_config_client_usecase below.

    Dropped during the merge (confirmed dead — not read anywhere in
    transform.py or orchestrator.py): the old bare
    use_case_definitions.column_mapping column, superseded by
    mapping_by_file.

    schema_id is nullable: a use-case definition can be imported
    (upsert_definition) before any operational config — and therefore
    before any schema_id — exists for that client+use-case. upsert_config
    fills schema_id in once a real config is committed.
    """
    __tablename__ = "pipeline_config"

    config_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    client_name: Mapped[str] = mapped_column(String(100), nullable=False)
    use_case_name: Mapped[str] = mapped_column(String(100), nullable=False)
    display_name: Mapped[Optional[str]] = mapped_column(String(150), nullable=True)

    schema_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("golden_schema_registry.schema_id", ondelete="RESTRICT"), nullable=True)

    # ── Operational control (formerly etl_pipeline_config) ──
    stop_pipeline: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("FALSE"))
    is_quarantined: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("FALSE"))
    ingestion_strategy: Mapped[str] = mapped_column(String(20), nullable=False, server_default=text("'OVERWRITE'"))
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("TRUE"))

    # ── Definition metadata (formerly use_case_definitions) ──
    tables_schema: Mapped[list] = mapped_column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))
    required_columns: Mapped[list] = mapped_column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))
    source_client: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    exported_at: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    # ── Mapping & transformation logic ──
    # column_mapping: the flat/fallback mapping (formerly client_pipeline_config.column_mapping)
    # mapping_by_file: per-source-filename override (formerly use_case_definitions.mapping_by_file)
    # Both confirmed live in transform.py — kept as distinct columns, not merged into each other.
    column_mapping: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    mapping_by_file: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    transformation_rules: Mapped[list] = mapped_column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))

    # Opt-in: if TRUE, a row missing this table's primary key is patched
    # with a unique placeholder (PATCH-<staging_id>) instead of the whole
    # table promotion being skipped for the batch. Defaults to FALSE so
    # existing skip-on-missing-PK behavior is preserved unless turned on.
    patch_missing_keys: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("FALSE"))

    created_at: Mapped[datetime] = mapped_column(TIMESTAMP, server_default=text("NOW()"), nullable=False)

    __table_args__ = (
        UniqueConstraint("client_name", "use_case_name", name="uq_pipeline_config_client_usecase"),
    )


# ===========================================================================
# 2. BATCH ORCHESTRATION LAYER
# ===========================================================================

class BatchMaster(Base):
    """
    MERGED 2026-06: formerly two tables — batch_master and
    batch_file_registry. Folded together because a batch is always
    exactly one file (confirmed — no multi-file-per-batch use case).

    client_id / use_case_id removed entirely. Resolve client/use-case
    via config_id -> pipeline_config.client_name / use_case_name.
    """
    __tablename__ = "batch_master"

    batch_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    config_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("pipeline_config.config_id", ondelete="RESTRICT"), nullable=False)

    # ── Folded in from batch_file_registry ──
    dataset_type: Mapped[str] = mapped_column(String(100), nullable=False)
    file_name: Mapped[str] = mapped_column(String(255), nullable=False)
    file_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    rows_extracted: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    # Renamed from the old batch_file_registry.status to avoid colliding
    # with this table's own batch-level `status` below — they track
    # different things (file extraction state vs. overall batch lifecycle).
    file_status: Mapped[str] = mapped_column(String(20), nullable=False, server_default=text("'PENDING'"))

    created_datetime: Mapped[datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text("NOW()"))
    completed_datetime: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP, nullable=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, server_default=text("'PENDING'"))
    run_by: Mapped[str] = mapped_column(String(100), nullable=False)

    __table_args__ = (
        CheckConstraint("status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'ABORTED')", name="chk_batch_status"),
    )


class EtlRunLog(Base):
    """
    Kept separate from batch_master on purpose — this is an append-only
    audit log (many rows per batch: STARTED/COMPLETED per step) and
    sits at a different grain than the 1-row-per-batch master record.
    """
    __tablename__ = "etl_run_log"

    log_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    batch_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("batch_master.batch_id", ondelete="CASCADE"), nullable=False)
    step_name: Mapped[str] = mapped_column(String(100), nullable=False)
    event_type: Mapped[str] = mapped_column(String(50), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    detail: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    logged_at: Mapped[datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text("NOW()"))


# ===========================================================================
# 3. EXTRACTION & STAGING LAYER
# ===========================================================================

class UnifiedRawStaging(Base):
    """
    Kept separate from unified_transformed_staging on purpose — row_hash
    here is used for duplicate detection, and that responsibility is
    intentionally isolated from the transformed/cleaned data.
    """
    __tablename__ = "unified_raw_staging"

    staging_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    batch_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("batch_master.batch_id", ondelete="CASCADE"), nullable=False)
    # file_id removed 2026-06: redundant once file_id == batch_id 1:1
    # (batch_file_registry was merged into batch_master). Use batch_id.

    dataset_type: Mapped[str] = mapped_column(String(100), nullable=False)
    table_name: Mapped[str] = mapped_column(String(100), nullable=False)
    row_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)

    # The actual schema-agnostic data
    raw_payload: Mapped[dict] = mapped_column(JSONB, nullable=False)

    # transformed_payload column removed 2026-06: confirmed dead — never
    # written or read by transform.py, which writes cleaned data straight
    # to unified_transformed_staging instead.

    created_datetime: Mapped[datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text("NOW()"))


class ReviewQueue(Base):
    __tablename__ = "review_queue"

    review_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    staging_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("unified_raw_staging.staging_id", ondelete="CASCADE"), nullable=False)
    rule_id: Mapped[str] = mapped_column(String(100), nullable=False)
    flagged_row: Mapped[dict] = mapped_column(JSONB, nullable=False)
    severity: Mapped[str] = mapped_column(String(20), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, server_default=text("'PENDING_REVIEW'"))


class UnifiedTransformedStaging(Base):
    """
    SILVER LAYER: Holds the fully transformed and validated JSON payloads.
    Strictly separated from the raw extraction layer (row_hash-based
    dedup lives on the raw side; this table is the load-phase source of
    truth for cleaned data).
    """
    __tablename__ = "unified_transformed_staging"

    transformed_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))

    # The 1:1 Audit Link back to the exact raw record
    staging_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("unified_raw_staging.staging_id", ondelete="CASCADE"), nullable=False, unique=True)

    # Duplicated for faster querying during the Load phase
    batch_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("batch_master.batch_id", ondelete="CASCADE"), nullable=False)
    table_name: Mapped[str] = mapped_column(String(100), nullable=False)

    # The cleaned data
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_datetime: Mapped[datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text("NOW()"))


# ===========================================================================
# 4. GOLDEN LAYER (STAR SCHEMA FOR POWER BI)
# ===========================================================================
# Kept as four separate dimension tables on purpose — this layer is built
# for Power BI, which relies on independent dimension tables for clean
# relationship modeling and slicer behavior against the fact table below.

class GoldenMasterCustomer(Base):
    """Dimension: Master_Customer"""
    __tablename__ = "golden_master_customer"

    # Matches Customer_ID and Customer_Name
    customer_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    customer_name: Mapped[str] = mapped_column(String(255), nullable=True)

    # ETL Lineage
    batch_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("batch_master.batch_id", ondelete="CASCADE"), nullable=False)
    loaded_at: Mapped[datetime] = mapped_column(TIMESTAMP, server_default=text("NOW()"))

class GoldenMasterProduct(Base):
    """Dimension: Master_Product"""
    __tablename__ = "golden_master_product"

    # Matches Product_ID, Product_Name, Product_Category, Product_Subcategory
    product_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    product_name: Mapped[str] = mapped_column(String(255), nullable=True)
    product_category: Mapped[str] = mapped_column(String(100), nullable=True)
    product_subcategory: Mapped[str] = mapped_column(String(100), nullable=True)

    batch_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("batch_master.batch_id", ondelete="CASCADE"), nullable=False)
    loaded_at: Mapped[datetime] = mapped_column(TIMESTAMP, server_default=text("NOW()"))

class GoldenMasterRegion(Base):
    """Dimension: Master_Region"""
    __tablename__ = "golden_master_region"

    # Matches Region_ID and Region
    region_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    region: Mapped[str] = mapped_column(String(100), nullable=True)

    batch_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("batch_master.batch_id", ondelete="CASCADE"), nullable=False)
    loaded_at: Mapped[datetime] = mapped_column(TIMESTAMP, server_default=text("NOW()"))

class GoldenMasterSalesperson(Base):
    """Dimension: Master_Salesperson"""
    __tablename__ = "golden_master_salesperson"

    # Matches Sales_Rep_ID, Sales_Rep_Name, Manager_Name
    sales_rep_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    sales_rep_name: Mapped[str] = mapped_column(String(255), nullable=True)
    manager_name: Mapped[str] = mapped_column(String(255), nullable=True)
    busness_head: Mapped[str] = mapped_column(String(255), nullable=True)

    batch_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("batch_master.batch_id", ondelete="CASCADE"), nullable=False)
    loaded_at: Mapped[datetime] = mapped_column(TIMESTAMP, server_default=text("NOW()"))

class GoldenSalesTransaction(Base):
    """Fact Table: Sales_Transaction"""
    __tablename__ = "golden_sales_transaction"

    # ─── COMPOSITE PRIMARY KEY ───
    transaction_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    product_id: Mapped[str] = mapped_column(String(100), ForeignKey("golden_master_product.product_id"), primary_key=True)

    # Foreign Keys referencing the Dimensions
    region_id: Mapped[str] = mapped_column(String(100), ForeignKey("golden_master_region.region_id"), nullable=True)
    customer_id: Mapped[str] = mapped_column(String(100), ForeignKey("golden_master_customer.customer_id"), nullable=True)
    sales_rep_id: Mapped[str] = mapped_column(String(100), ForeignKey("golden_master_salesperson.sales_rep_id"), nullable=True)

    # Base Data
    transaction_date: Mapped[date] = mapped_column(Date, nullable=True)

    # Measures
    gross_sales: Mapped[float] = mapped_column(Numeric(18, 2), nullable=True)
    discount_amount: Mapped[float] = mapped_column(Numeric(18, 2), nullable=True)
    total_sales_amount: Mapped[float] = mapped_column(Numeric(18, 2), nullable=True)
    cogs: Mapped[float] = mapped_column(Numeric(18, 2), nullable=True)
    cost: Mapped[float] = mapped_column(Numeric(18, 2), nullable=True)
    quantity: Mapped[float] = mapped_column(Numeric(18, 2), nullable=True)

    # Metadata
    transaction_type: Mapped[str] = mapped_column(String(50), nullable=True)
    base_currency: Mapped[str] = mapped_column(String(10), nullable=True)
    exchange_rate: Mapped[float] = mapped_column(Numeric(18, 4), nullable=True)

    # System Data
    batch_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("batch_master.batch_id", ondelete="CASCADE"))
    loaded_at: Mapped[datetime] = mapped_column(TIMESTAMP, server_default=text("NOW()"))