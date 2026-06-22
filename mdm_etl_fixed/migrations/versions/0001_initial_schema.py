"""initial_schema

Single clean migration replacing all previous versions.
Creates the full schema from scratch matching models.py exactly.

Revision ID: 0001_initial_schema
Revises: 
Create Date: 2026-06-18

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = '0001_initial_schema'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:

    # ── 1. golden_schema_registry ─────────────────────────────────────────
    op.create_table(
        'golden_schema_registry',
        sa.Column('schema_id', postgresql.UUID(as_uuid=True), server_default=sa.text('gen_random_uuid()'), nullable=False),
        sa.Column('dataset_type', sa.String(100), nullable=False),
        sa.Column('version', sa.String(50), nullable=False),
        sa.Column('mandatory_columns', postgresql.JSONB(), server_default=sa.text("'[]'::jsonb"), nullable=False),
        sa.Column('expected_data_types', postgresql.JSONB(), server_default=sa.text("'{}'::jsonb"), nullable=False),
        sa.Column('created_at', postgresql.TIMESTAMP(), server_default=sa.text('NOW()'), nullable=False),
        sa.PrimaryKeyConstraint('schema_id'),
        sa.UniqueConstraint('dataset_type', 'version', name='uq_golden_schema_dataset_version'),
    )

    # ── 2. etl_pipeline_config ────────────────────────────────────────────
    op.create_table(
        'etl_pipeline_config',
        sa.Column('client_id', sa.String(50), nullable=False),
        sa.Column('use_case_id', sa.String(50), nullable=False),
        sa.Column('stop_pipeline', sa.Boolean(), server_default=sa.text('FALSE'), nullable=False),
        sa.Column('is_quarantined', sa.Boolean(), server_default=sa.text('FALSE'), nullable=False),
        sa.Column('ingestion_strategy', sa.String(20), server_default=sa.text("'OVERWRITE'"), nullable=False),
        sa.Column('is_active', sa.Boolean(), server_default=sa.text('TRUE'), nullable=False),
        sa.PrimaryKeyConstraint('client_id', 'use_case_id'),
    )

    # ── 3. client_pipeline_config ─────────────────────────────────────────
    op.create_table(
        'client_pipeline_config',
        sa.Column('config_id', postgresql.UUID(as_uuid=True), server_default=sa.text('gen_random_uuid()'), nullable=False),
        sa.Column('client_id', sa.String(50), nullable=False),
        sa.Column('use_case_id', sa.String(50), nullable=False),
        sa.Column('schema_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('column_mapping', postgresql.JSONB(), server_default=sa.text("'{}'::jsonb"), nullable=False),
        sa.Column('transformation_rules', postgresql.JSONB(), server_default=sa.text("'[]'::jsonb"), nullable=False),
        sa.Column('is_active', sa.Boolean(), server_default=sa.text('TRUE'), nullable=False),
        sa.Column('patch_missing_keys', sa.Boolean(), server_default=sa.text('FALSE'), nullable=False),
        sa.ForeignKeyConstraint(['schema_id'], ['golden_schema_registry.schema_id'], ondelete='RESTRICT'),
        sa.ForeignKeyConstraint(['client_id', 'use_case_id'], ['etl_pipeline_config.client_id', 'etl_pipeline_config.use_case_id'], ondelete='RESTRICT'),
        sa.PrimaryKeyConstraint('config_id'),
    )

    # ── 4. use_case_definitions ───────────────────────────────────────────
    op.create_table(
        'use_case_definitions',
        sa.Column('use_case_id', postgresql.UUID(as_uuid=True), server_default=sa.text('gen_random_uuid()'), nullable=False),
        sa.Column('client_id', sa.String(100), nullable=False),
        sa.Column('use_case_name', sa.String(100), nullable=False),
        sa.Column('display_name', sa.String(150), nullable=True),
        sa.Column('tables_schema', postgresql.JSONB(), server_default=sa.text("'[]'::jsonb"), nullable=False),
        sa.Column('column_mapping', postgresql.JSONB(), server_default=sa.text("'{}'::jsonb"), nullable=False),
        sa.Column('required_columns', postgresql.JSONB(), server_default=sa.text("'[]'::jsonb"), nullable=False),
        sa.Column('mapping_by_file', postgresql.JSONB(), server_default=sa.text("'{}'::jsonb"), nullable=False),
        sa.Column('source_client', sa.String(100), nullable=True),
        sa.Column('exported_at', sa.String(50), nullable=True),
        sa.Column('created_at', postgresql.TIMESTAMP(), server_default=sa.text('NOW()'), nullable=False),
        sa.PrimaryKeyConstraint('use_case_id'),
        sa.UniqueConstraint('client_id', 'use_case_name', name='uq_client_use_case'),
    )

    # ── 5. batch_master ───────────────────────────────────────────────────
    op.create_table(
        'batch_master',
        sa.Column('batch_id', postgresql.UUID(as_uuid=True), server_default=sa.text('gen_random_uuid()'), nullable=False),
        sa.Column('client_id', sa.String(50), nullable=False),
        sa.Column('use_case_id', sa.String(50), nullable=False),
        sa.Column('config_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('created_datetime', postgresql.TIMESTAMP(), server_default=sa.text('NOW()'), nullable=False),
        sa.Column('completed_datetime', postgresql.TIMESTAMP(), nullable=True),
        sa.Column('status', sa.String(20), server_default=sa.text("'PENDING'"), nullable=False),
        sa.Column('run_by', sa.String(100), nullable=False),
        sa.ForeignKeyConstraint(['config_id'], ['client_pipeline_config.config_id'], ondelete='RESTRICT'),
        sa.PrimaryKeyConstraint('batch_id'),
        sa.CheckConstraint("status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'ABORTED')", name='chk_batch_status'),
    )

    # ── 6. batch_file_registry ────────────────────────────────────────────
    op.create_table(
        'batch_file_registry',
        sa.Column('file_id', postgresql.UUID(as_uuid=True), server_default=sa.text('gen_random_uuid()'), nullable=False),
        sa.Column('batch_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('dataset_type', sa.String(100), nullable=False),
        sa.Column('file_name', sa.String(255), nullable=False),
        sa.Column('file_hash', sa.String(64), nullable=False),
        sa.Column('rows_extracted', sa.Integer(), nullable=True),
        sa.Column('status', sa.String(20), server_default=sa.text("'PENDING'"), nullable=False),
        sa.ForeignKeyConstraint(['batch_id'], ['batch_master.batch_id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('file_id'),
    )

    # ── 7. etl_run_log ────────────────────────────────────────────────────
    op.create_table(
        'etl_run_log',
        sa.Column('log_id', postgresql.UUID(as_uuid=True), server_default=sa.text('gen_random_uuid()'), nullable=False),
        sa.Column('batch_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('step_name', sa.String(100), nullable=False),
        sa.Column('event_type', sa.String(50), nullable=False),
        sa.Column('status', sa.String(20), nullable=False),
        sa.Column('detail', postgresql.JSONB(), nullable=True),
        sa.Column('logged_at', postgresql.TIMESTAMP(), server_default=sa.text('NOW()'), nullable=False),
        sa.ForeignKeyConstraint(['batch_id'], ['batch_master.batch_id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('log_id'),
    )

    # ── 8. unified_raw_staging ────────────────────────────────────────────
    op.create_table(
        'unified_raw_staging',
        sa.Column('staging_id', postgresql.UUID(as_uuid=True), server_default=sa.text('gen_random_uuid()'), nullable=False),
        sa.Column('batch_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('file_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('dataset_type', sa.String(100), nullable=False),
        sa.Column('table_name', sa.String(100), nullable=False),
        sa.Column('row_hash', sa.String(64), nullable=True),
        sa.Column('raw_payload', postgresql.JSONB(), nullable=False),
        sa.Column('transformed_payload', postgresql.JSONB(), nullable=True),
        sa.Column('created_datetime', postgresql.TIMESTAMP(), server_default=sa.text('NOW()'), nullable=False),
        sa.ForeignKeyConstraint(['batch_id'], ['batch_master.batch_id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['file_id'], ['batch_file_registry.file_id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('staging_id'),
    )

    # ── 9. review_queue ───────────────────────────────────────────────────
    op.create_table(
        'review_queue',
        sa.Column('review_id', postgresql.UUID(as_uuid=True), server_default=sa.text('gen_random_uuid()'), nullable=False),
        sa.Column('staging_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('rule_id', sa.String(100), nullable=False),
        sa.Column('flagged_row', postgresql.JSONB(), nullable=False),
        sa.Column('severity', sa.String(20), nullable=False),
        sa.Column('status', sa.String(20), server_default=sa.text("'PENDING_REVIEW'"), nullable=False),
        sa.ForeignKeyConstraint(['staging_id'], ['unified_raw_staging.staging_id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('review_id'),
    )

    # ── 10. unified_transformed_staging ──────────────────────────────────
    op.create_table(
        'unified_transformed_staging',
        sa.Column('transformed_id', postgresql.UUID(as_uuid=True), server_default=sa.text('gen_random_uuid()'), nullable=False),
        sa.Column('staging_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('batch_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('table_name', sa.String(100), nullable=False),
        sa.Column('payload', postgresql.JSONB(), nullable=False),
        sa.Column('created_datetime', postgresql.TIMESTAMP(), server_default=sa.text('NOW()'), nullable=False),
        sa.ForeignKeyConstraint(['staging_id'], ['unified_raw_staging.staging_id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['batch_id'], ['batch_master.batch_id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('transformed_id'),
        sa.UniqueConstraint('staging_id'),
    )

    # ── 11. golden_master_customer ────────────────────────────────────────
    op.create_table(
        'golden_master_customer',
        sa.Column('customer_id', sa.String(100), nullable=False),
        sa.Column('customer_name', sa.String(255), nullable=True),
        sa.Column('batch_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('loaded_at', postgresql.TIMESTAMP(), server_default=sa.text('NOW()')),
        sa.ForeignKeyConstraint(['batch_id'], ['batch_master.batch_id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('customer_id'),
    )

    # ── 12. golden_master_product ─────────────────────────────────────────
    op.create_table(
        'golden_master_product',
        sa.Column('product_id', sa.String(100), nullable=False),
        sa.Column('product_name', sa.String(255), nullable=True),
        sa.Column('product_category', sa.String(100), nullable=True),
        sa.Column('product_subcategory', sa.String(100), nullable=True),
        sa.Column('batch_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('loaded_at', postgresql.TIMESTAMP(), server_default=sa.text('NOW()')),
        sa.ForeignKeyConstraint(['batch_id'], ['batch_master.batch_id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('product_id'),
    )

    # ── 13. golden_master_region ──────────────────────────────────────────
    op.create_table(
        'golden_master_region',
        sa.Column('region_id', sa.String(100), nullable=False),
        sa.Column('region', sa.String(100), nullable=True),
        sa.Column('batch_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('loaded_at', postgresql.TIMESTAMP(), server_default=sa.text('NOW()')),
        sa.ForeignKeyConstraint(['batch_id'], ['batch_master.batch_id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('region_id'),
    )

    # ── 14. golden_master_salesperson ─────────────────────────────────────
    op.create_table(
        'golden_master_salesperson',
        sa.Column('sales_rep_id', sa.String(100), nullable=False),
        sa.Column('sales_rep_name', sa.String(255), nullable=True),
        sa.Column('manager_name', sa.String(255), nullable=True),
        sa.Column('busness_head', sa.String(255), nullable=True),
        sa.Column('batch_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('loaded_at', postgresql.TIMESTAMP(), server_default=sa.text('NOW()')),
        sa.ForeignKeyConstraint(['batch_id'], ['batch_master.batch_id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('sales_rep_id'),
    )

    # ── 15. golden_sales_transaction ──────────────────────────────────────
    op.create_table(
        'golden_sales_transaction',
        sa.Column('transaction_id', sa.String(100), nullable=False),
        sa.Column('product_id', sa.String(100), nullable=False),
        sa.Column('region_id', sa.String(100), nullable=True),
        sa.Column('customer_id', sa.String(100), nullable=True),
        sa.Column('sales_rep_id', sa.String(100), nullable=True),
        sa.Column('transaction_date', sa.Date(), nullable=True),
        sa.Column('gross_sales', sa.Numeric(18, 2), nullable=True),
        sa.Column('discount_amount', sa.Numeric(18, 2), nullable=True),
        sa.Column('total_sales_amount', sa.Numeric(18, 2), nullable=True),
        sa.Column('cogs', sa.Numeric(18, 2), nullable=True),
        sa.Column('cost', sa.Numeric(18, 2), nullable=True),
        sa.Column('quantity', sa.Numeric(18, 2), nullable=True),
        sa.Column('transaction_type', sa.String(50), nullable=True),
        sa.Column('base_currency', sa.String(10), nullable=True),
        sa.Column('exchange_rate', sa.Numeric(18, 4), nullable=True),
        sa.Column('batch_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('loaded_at', postgresql.TIMESTAMP(), server_default=sa.text('NOW()')),
        sa.ForeignKeyConstraint(['product_id'], ['golden_master_product.product_id']),
        sa.ForeignKeyConstraint(['region_id'], ['golden_master_region.region_id']),
        sa.ForeignKeyConstraint(['customer_id'], ['golden_master_customer.customer_id']),
        sa.ForeignKeyConstraint(['sales_rep_id'], ['golden_master_salesperson.sales_rep_id']),
        sa.ForeignKeyConstraint(['batch_id'], ['batch_master.batch_id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('transaction_id', 'product_id'),
    )


def downgrade() -> None:
    op.drop_table('golden_sales_transaction')
    op.drop_table('golden_master_salesperson')
    op.drop_table('golden_master_region')
    op.drop_table('golden_master_product')
    op.drop_table('golden_master_customer')
    op.drop_table('unified_transformed_staging')
    op.drop_table('review_queue')
    op.drop_table('unified_raw_staging')
    op.drop_table('etl_run_log')
    op.drop_table('batch_file_registry')
    op.drop_table('batch_master')
    op.drop_table('use_case_definitions')
    op.drop_table('client_pipeline_config')
    op.drop_table('etl_pipeline_config')
    op.drop_table('golden_schema_registry')
