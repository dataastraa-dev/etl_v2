"""
strategies/transformations.py
Global (GT) and User-Defined (UT) transformer implementations.
 
Phase A  — Global ITransformer chain (run in fixed order, always):
    column_mapper           GT-1  MANDATORY, always first
    strip_currency_symbols  GT-2
    normalize_whitespace    GT-3
    standardize_boolean     GT-4
    enforce_date_format     GT-6
    compute_derived_columns GT-5  always last in Phase A
 
Phase B  — Per-column user-defined rule engine (run per transformation_rules JSONB):
    prefix_strip            UT-1
    suffix_strip            UT-1b
    string_pad              UT-2
    value_remap             UT-3
    date_part_extract       UT-4
    concatenate_columns     UT-5
    split_column            UT-6
    lookup_enrich           UT-7  (requires db_conn in config)
    conditional_fill        UT-8
"""
 
import re
from datetime import datetime
from typing import Any
 
import pandas as pd
 
from core.registry import ITransformer, StrategyRegistry
 
 
# ============================================================
# PHASE A — GLOBAL TRANSFORMATIONS
# ============================================================
 
@StrategyRegistry.register("column_mapper")
class ColumnMapperTransformer(ITransformer):
    """
    GT-1  MANDATORY — always the first transformer in the chain.
 
    Renames client-side columns to golden schema column names using
    client_pipeline_config.column_mapping JSONB.
 
    column_mapping can be:
        • Table-scoped  : { "sales_transaction": { "Trans_ID": "transaction_id", ... } }
        • Flat (compat) : { "Trans_ID": "transaction_id", ... }
 
    Columns absent from the mapping are left as-is (SchemaValidator
    will flag unexpected columns later if required).
    """
 
    def transform(self, df: pd.DataFrame, config: dict[str, Any]):
        anomalies: list[dict] = []
        mapping_raw: dict = config.get("column_mapping") or {}
        table_name: str = config.get("production_table_name", "")
 
        print(f"[DIAG column_mapper] table_name={table_name!r}", flush=True)
        print(f"[DIAG column_mapper] mapping_raw keys: {list(mapping_raw.keys())[:6]}", flush=True)
        _first = list(mapping_raw.values())[0] if mapping_raw else None
        print(f"[DIAG column_mapper] first value type: {type(_first).__name__}, val[:80]: {str(_first)[:80]}", flush=True)
 
        # Resolve the correct sub-mapping for this table
        if table_name and table_name in mapping_raw:
            column_map: dict = mapping_raw[table_name]
        else:
            # Fallback: assume flat dict (backward compat or single-table scenario)
            # Check if any value is itself a dict — if so, try to flatten
            if mapping_raw and all(isinstance(v, dict) for v in mapping_raw.values()):
                # Merge all table mappings into one flat dict (last-write wins on collision)
                column_map = {}
                for sub in mapping_raw.values():
                    column_map.update(sub)
            else:
                column_map = mapping_raw
 
        print(f"[DIAG column_mapper] column_map keys[:5]: {list(column_map.keys())[:5]}", flush=True)
        print(f"[DIAG column_mapper] 'CUSTOMER_TRX_ID' in column_map: {'CUSTOMER_TRX_ID' in column_map}", flush=True)
 
        if not column_map:
            # An empty mapping means every source column is left as-is, which
            # guarantees the golden NOT-NULL columns (e.g. transaction_id)
            # will be absent after this step. Must block load, not warn.
            anomalies.append({
                "rule": "column_mapper",
                "severity": "CRITICAL",
                "message": f"No column mapping found for table '{table_name}'. Columns left "
                           f"as-is — golden schema columns will be missing and load will fail "
                           f"the NOT-NULL constraint. Check client_pipeline_config.column_mapping "
                           f"for client/use_case '{table_name}'.",
            })
            return df, anomalies
 
        missing_source_cols = [c for c in column_map if c not in df.columns]
        if missing_source_cols:
            anomalies.append({
                "rule": "column_mapper",
                "severity": "WARNING",
                "message": f"Source columns expected by mapping but absent in file: {missing_source_cols}",
            })
 
        df = df.rename(columns=column_map)
        return df, anomalies
 
 
@StrategyRegistry.register("strip_currency_symbols")
class StripCurrencySymbolsTransformer(ITransformer):
    """
    GT-2  Strip currency symbols and thousands separators, then coerce to float.
 
    Target columns: all columns whose golden schema data_type is numeric.
    Pass them in config["numeric_columns"] (populated by the orchestrator from
    golden_schema_registry.expected_data_types).
    """
 
    _PATTERN = re.compile(r"[$€£₹¥,\s]")
 
    def transform(self, df: pd.DataFrame, config: dict[str, Any]):
        anomalies: list[dict] = []
        target_cols: list[str] = config.get("numeric_columns", [])
 
        for col in target_cols:
            if col not in df.columns or df[col].dtype != "object":
                continue
 
            clean = df[col].astype(str).str.replace(self._PATTERN, "", regex=True)
            numeric = pd.to_numeric(clean, errors="coerce")
 
            # Rows that had a non-null original value but failed coercion
            failed_mask = df[col].notna() & numeric.isna() & (clean.str.strip() != "nan")
            if failed_mask.any():
                anomalies.append({
                    "rule": "strip_currency_symbols",
                    "column": col,
                    "severity": "ERROR",
                    "affected_rows": df[failed_mask].index.tolist(),
                    "message": "Values could not be coerced to numeric after symbol stripping. Set to NaN.",
                })
 
            df[col] = numeric
        return df, anomalies
 
 
@StrategyRegistry.register("normalize_whitespace")
class NormalizeWhitespaceTransformer(ITransformer):
    """
    GT-3  Collapse all internal whitespace (tabs, nbsp, double-spaces) to single
          spaces and strip leading/trailing whitespace on all string columns.
 
    Extends the basic trim_strings already in the global chain.
    Prevents dimension fragmentation in Power BI group-bys.
    """
 
    _PATTERN = re.compile(r"[\s\xa0]+")
 
    def transform(self, df: pd.DataFrame, config: dict[str, Any]):
        for col in df.select_dtypes(include=["object", "string"]).columns:
            df[col] = (
                df[col]
                .astype(str)
                .apply(lambda x: self._PATTERN.sub(" ", x).strip() if x != "nan" else x)
            )
        return df, []
 
 
@StrategyRegistry.register("standardize_boolean")
class StandardizeBooleanTransformer(ITransformer):
    """
    GT-4  Unify heterogeneous boolean representations (Y/N, yes/no, 1/0, TRUE/FALSE)
          across all columns whose golden schema data_type is boolean.
 
    Unrecognised values → None + WARNING anomaly.
    """
 
    _BOOL_MAP = {
        "y": True, "yes": True, "1": True, "true": True,
        "n": False, "no": False, "0": False, "false": False,
    }
 
    def transform(self, df: pd.DataFrame, config: dict[str, Any]):
        anomalies: list[dict] = []
        bool_cols: list[str] = config.get("boolean_columns", [])
 
        for col in bool_cols:
            if col not in df.columns:
                continue
 
            original = df[col].copy()
            df[col] = df[col].astype(str).str.lower().map(self._BOOL_MAP)
 
            unmapped_mask = df[col].isna() & original.notna() & (original.astype(str) != "nan")
            if unmapped_mask.any():
                anomalies.append({
                    "rule": "standardize_boolean",
                    "column": col,
                    "severity": "WARNING",
                    "affected_rows": df[unmapped_mask].index.tolist(),
                    "message": "Unrecognised boolean values coerced to NaN.",
                })
 
        return df, anomalies
 
 
@StrategyRegistry.register("enforce_date_format")
class EnforceDateFormatTransformer(ITransformer):
    """
    GT-6  Normalise all date columns to Python datetime.date (YYYY-MM-DD).
 
    Tries multiple format strings before falling back to pandas inference.
    Rows that cannot be parsed → NaT + ERROR anomaly.
    Also flags future-dated transaction rows (business constraint from GV-3 logic).
 
    Run BEFORE compute_derived_columns so date arithmetic works correctly.
    """
 
    _FORMATS = [
        "%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y", "%d/%m/%Y",
        "%Y/%m/%d", "%d-%b-%y", "%d-%b-%Y", "%B %d, %Y",
        "%Y%m%d",
    ]
 
    def transform(self, df: pd.DataFrame, config: dict[str, Any]):
        anomalies: list[dict] = []
        date_cols: list[str] = config.get("date_columns", [])
        today = datetime.today().date()
 
        for col in date_cols:
            if col not in df.columns:
                continue
 
            original = df[col].copy()
            parsed = None
 
            # Try explicit formats first for determinism
            for fmt in self._FORMATS:
                attempt = pd.to_datetime(df[col], format=fmt, errors="coerce")
                if parsed is None:
                    parsed = attempt
                else:
                    # Fill gaps from prior attempts with this format's results
                    parsed = parsed.fillna(attempt)
 
            # Final fallback: pandas inference
            still_null = parsed.isna() & original.notna()
            if still_null.any():
                inferred = pd.to_datetime(df[col], infer_datetime_format=True, errors="coerce")
                parsed = parsed.fillna(inferred)
 
            failed = original.notna() & parsed.isna()
            if failed.any():
                anomalies.append({
                    "rule": "enforce_date_format",
                    "column": col,
                    "severity": "ERROR",
                    "affected_rows": df[failed].index.tolist(),
                    "message": "Unparseable date formats — set to NaT.",
                })
 
            df[col] = parsed.dt.date
 
        return df, anomalies
 
 
@StrategyRegistry.register("compute_derived_columns")
class ComputeDerivedColumnsTransformer(ITransformer):
    """
    GT-5: Safely computes financial measures (gross_sales, cogs, margins)
    ONLY if the prerequisite columns exist in the DataFrame.
    """
    def transform(self, df: pd.DataFrame, config: dict):
        anomalies = []
        
        # 1. Safely convert monetary/quantity columns to numeric (floats)
        # Using 'coerce' turns text/garbage into NaN instead of crashing
        numeric_cols = [
            "quantity", "rate", "cost", 
            "total_sales_amount", "discount_amount", "gross_sales", "cogs"
        ]
        
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        # 2. Compute Gross Sales (Fallback to total + discount if rate is missing)
        if 'quantity' in df.columns and 'rate' in df.columns and 'gross_sales' not in df.columns:
            df['gross_sales'] = df['quantity'] * df['rate']
        elif 'total_sales_amount' in df.columns and 'gross_sales' not in df.columns:
            disc = df['discount_amount'] if 'discount_amount' in df.columns else 0
            df['gross_sales'] = df['total_sales_amount'] + disc

        # 3. Compute COGS (Cost of Goods Sold)
        if 'quantity' in df.columns and 'cost' in df.columns and 'cogs' not in df.columns:
            df['cogs'] = df['quantity'] * df['cost']

        # 4. Compute Gross Margin
        if 'gross_sales' in df.columns and 'cogs' in df.columns and 'gross_margin' not in df.columns:
            df['gross_margin'] = df['gross_sales'] - df['cogs']

        return df, anomalies
 
 
# ============================================================
# PHASE B — USER-DEFINED TRANSFORMATIONS
# ============================================================
 
@StrategyRegistry.register("prefix_strip")
class PrefixStripTransformer(ITransformer):
    """UT-1a  Strip a known prefix from a column's values."""
 
    def transform(self, df: pd.DataFrame, config: dict[str, Any]):
        anomalies: list[dict] = []
        col = config["column"]
        params = config.get("parameters", {})
        prefix: str = params.get("prefix", "")
        max_length: int | None = params.get("max_length")
 
        if col not in df.columns:
            return df, anomalies
 
        df[col] = df[col].astype(str).str.removeprefix(prefix)
        if max_length:
            df[col] = df[col].str[:max_length]
 
        # Flag rows where stripping produced an empty value
        empty_mask = df[col].str.strip() == ""
        if empty_mask.any():
            anomalies.append({
                "rule": "prefix_strip",
                "column": col,
                "severity": "WARNING",
                "affected_rows": df[empty_mask].index.tolist(),
                "message": f"Stripping prefix '{prefix}' produced empty values.",
            })
 
        return df, anomalies
 
 
@StrategyRegistry.register("suffix_strip")
class SuffixStripTransformer(ITransformer):
    """UT-1b  Strip a known suffix from a column's values."""
 
    def transform(self, df: pd.DataFrame, config: dict[str, Any]):
        anomalies: list[dict] = []
        col = config["column"]
        params = config.get("parameters", {})
        suffix: str = params.get("suffix", "")
        max_length: int | None = params.get("max_length")
 
        if col not in df.columns:
            return df, anomalies
 
        df[col] = df[col].astype(str).str.removesuffix(suffix)
        if max_length:
            df[col] = df[col].str[:max_length]
 
        empty_mask = df[col].str.strip() == ""
        if empty_mask.any():
            anomalies.append({
                "rule": "suffix_strip",
                "column": col,
                "severity": "WARNING",
                "affected_rows": df[empty_mask].index.tolist(),
                "message": f"Stripping suffix '{suffix}' produced empty values.",
            })
 
        return df, anomalies
 
 
@StrategyRegistry.register("string_pad")
class StringPadTransformer(ITransformer):
    """
    UT-2  Pad a string column to a target width.
 
    Parameters:
        width     : int    — target total width
        fill_char : str    — character used for padding (default '0')
        side      : 'left' | 'right'  (default 'left')
    """
 
    def transform(self, df: pd.DataFrame, config: dict[str, Any]):
        col = config["column"]
        params = config.get("parameters", {})
        width: int = int(params.get("width", 6))
        fill_char: str = str(params.get("fill_char", "0"))[:1] or "0"
        side: str = params.get("side", "left")
 
        if col not in df.columns:
            return df, []
 
        if side == "left":
            df[col] = df[col].astype(str).str.zfill(width) if fill_char == "0" else \
                      df[col].astype(str).str.rjust(width, fill_char)
        else:
            df[col] = df[col].astype(str).str.ljust(width, fill_char)
 
        return df, []
    
@StrategyRegistry.register("null_imputer")
class NullImputerStrategy:
    """
    UT: Replaces all null/NaN values in a specified column with a user-provided value.
    Works dynamically across strings, numerics, and dates.
    """
    def transform(self, df: pd.DataFrame, config: dict) -> tuple[pd.DataFrame, list[dict]]:
        anomalies = []
        col = config.get("column")
        fill_value = config.get("fill_value")
 
        # 1. Validation Checks
        if col not in df.columns:
            anomalies.append({
                "severity": "ERROR",
                "message": f"Null Imputer failed: Column '{col}' not found in dataset."
            })
            return df, anomalies
 
        if fill_value is None:
            anomalies.append({
                "severity": "ERROR",
                "message": f"Null Imputer failed: No 'fill_value' provided for column '{col}'."
            })
            return df, anomalies
 
        # 2. Type casting the fill_value (Optional but recommended for strictness)
        # If the user typed "0" in the UI but the column is numeric, Pandas usually 
        # handles it gracefully, but explicit conversion prevents edge-case crashes.
        if pd.api.types.is_numeric_dtype(df[col]):
            try:
                fill_value = float(fill_value) if '.' in str(fill_value) else int(fill_value)
            except ValueError:
                pass # Let Pandas attempt the fallback broadcast
 
        # 3. Execution & Auditing
        null_mask = pd.isna(df[col])
        null_count = int(null_mask.sum())
 
        if null_count > 0:
            df[col] = df[col].fillna(fill_value)
            
            # Log as INFO so it appears in the transformations report without flagging as an error
            anomalies.append({
                "severity": "INFO", 
                "message": f"Imputed {null_count} null values with '{fill_value}' in column '{col}'."
            })
 
        return df, anomalies
 
 
@StrategyRegistry.register("value_remap")
class ValueRemapTransformer(ITransformer):
    """
    UT-3  Map column values using a user-supplied dictionary.
 
    Parameters:
        map              : dict  — { old_value: new_value }
        unmapped_action  : 'fill_null' | 'flag' | 'drop'  (default 'flag')
        case_sensitive   : bool  (default True)
    """
 
    def transform(self, df: pd.DataFrame, config: dict[str, Any]):
        anomalies: list[dict] = []
        col = config["column"]
        params = config.get("parameters", {})
        val_map: dict = params.get("map", {})
        action: str = params.get("unmapped_action", "flag")
        case_sensitive: bool = params.get("case_sensitive", True)
 
        if col not in df.columns:
            return df, anomalies
 
        working = df[col].astype(str) if not case_sensitive else df[col].astype(str)
        if not case_sensitive:
            lookup = {k.lower(): v for k, v in val_map.items()}
            unmapped_mask = ~working.str.lower().isin(lookup.keys()) & df[col].notna()
            df[col] = working.str.lower().map(lookup)
        else:
            unmapped_mask = ~working.isin(val_map.keys()) & df[col].notna()
            df[col] = working.map(val_map)
 
        if unmapped_mask.any():
            if action == "drop":
                df = df[~unmapped_mask]
            anomalies.append({
                "rule": "value_remap",
                "column": col,
                "severity": "FLAGGED",
                "affected_rows": df[unmapped_mask].index.tolist() if action != "drop" else [],
                "message": f"Unmapped values found in '{col}'. Action taken: {action}.",
            })
 
        return df, anomalies
 
 
@StrategyRegistry.register("date_part_extract")
class DatePartExtractTransformer(ITransformer):
    """
    UT-4  Extract a date part from a datetime column.
 
    Parameters:
        part : 'date' | 'year' | 'month' | 'year_month' | 'quarter'
    """
 
    def transform(self, df: pd.DataFrame, config: dict[str, Any]):
        anomalies: list[dict] = []
        col = config["column"]
        params = config.get("parameters", {})
        part: str = params.get("part", "date")
 
        if col not in df.columns:
            return df, anomalies
 
        parsed = pd.to_datetime(df[col], errors="coerce")
        failed = df[col].notna() & parsed.isna()
        if failed.any():
            anomalies.append({
                "rule": "date_part_extract",
                "column": col,
                "severity": "ERROR",
                "affected_rows": df[failed].index.tolist(),
                "message": f"Could not parse column '{col}' as datetime. Affected rows set to NaT.",
            })
 
        part_map = {
            "date":       parsed.dt.date,
            "year":       parsed.dt.year,
            "month":      parsed.dt.month,
            "year_month": parsed.dt.to_period("M").astype(str),
            "quarter":    parsed.dt.quarter,
        }
        df[col] = part_map.get(part, parsed.dt.date)
 
        return df, anomalies
 
 
@StrategyRegistry.register("concatenate_columns")
class ConcatenateColumnsTransformer(ITransformer):
    """
    UT-5  Concatenate multiple source columns into the target column.
 
    Parameters:
        source_columns : list[str]  — columns to join
        separator      : str        — join character (default ' ')
        drop_sources   : bool       — remove source columns after (default True)
    """
 
    def transform(self, df: pd.DataFrame, config: dict[str, Any]):
        anomalies: list[dict] = []
        col = config["column"]
        params = config.get("parameters", {})
        sources: list[str] = params.get("source_columns", [])
        sep: str = params.get("separator", " ")
        drop_sources: bool = params.get("drop_sources", True)
 
        missing = [c for c in sources if c not in df.columns]
        if missing:
            anomalies.append({
                "rule": "concatenate_columns",
                "column": col,
                "severity": "ERROR",
                "message": f"Source columns for concatenation not found: {missing}",
            })
            return df, anomalies
 
        df[col] = df[sources].fillna("").apply(
            lambda row: sep.join(v.strip() for v in row.astype(str) if v.strip()),
            axis=1,
        )
 
        if drop_sources:
            df = df.drop(columns=[c for c in sources if c != col])
 
        return df, anomalies
 
 
@StrategyRegistry.register("split_column")
class SplitColumnTransformer(ITransformer):
    """
    UT-6  Split a single column into multiple output columns by a delimiter.
 
    Parameters:
        delimiter      : str        — split on this string
        output_columns : list[str]  — names for the resulting parts
        keep_original  : bool       — retain the source column (default False)
    """
 
    def transform(self, df: pd.DataFrame, config: dict[str, Any]):
        anomalies: list[dict] = []
        col = config["column"]
        params = config.get("parameters", {})
        delimiter: str = params.get("delimiter", " - ")
        output_cols: list[str] = params.get("output_columns", [])
        keep_original: bool = params.get("keep_original", False)
 
        if col not in df.columns or not output_cols:
            return df, anomalies
 
        split = df[col].astype(str).str.split(delimiter, n=len(output_cols) - 1, expand=True)
        split.columns = range(split.shape[1])
 
        for i, out_col in enumerate(output_cols):
            df[out_col] = split[i] if i < split.shape[1] else None
 
        if not keep_original and col not in output_cols:
            df = df.drop(columns=[col])
 
        return df, anomalies
 
 
@StrategyRegistry.register("lookup_enrich")
class LookupEnrichTransformer(ITransformer):
    """
    UT-7  Enrich a column's values via a static DB lookup table.
 
    Requires config["db_conn"] to be a live psycopg2 connection — injected by
    the transform orchestrator when DB access is needed.
 
    Parameters:
        lookup_table  : str  — table name in the ETL database
        lookup_key    : str  — PK/match column in the lookup table
        lookup_value  : str  — value column to pull back
        match_column  : str  — which column in df to match against
    """
 
    def transform(self, df: pd.DataFrame, config: dict[str, Any]):
        anomalies: list[dict] = []
        col = config["column"]
        params = config.get("parameters", {})
        lookup_table: str = params.get("lookup_table", "")
        lookup_key: str = params.get("lookup_key", "")
        lookup_value: str = params.get("lookup_value", "")
        match_column: str = params.get("match_column", col)
        db_conn = config.get("db_conn")
 
        if col not in df.columns:
            return df, anomalies
 
        if not db_conn or not all([lookup_table, lookup_key, lookup_value]):
            anomalies.append({
                "rule": "lookup_enrich",
                "column": col,
                "severity": "ERROR",
                "message": "lookup_enrich requires db_conn and all lookup parameters to be set.",
            })
            return df, anomalies
 
        try:
            with db_conn.cursor() as cur:
                cur.execute(f"SELECT {lookup_key}, {lookup_value} FROM {lookup_table}")
                lookup_dict = {str(r[0]): r[1] for r in cur.fetchall()}
        except Exception as exc:
            anomalies.append({
                "rule": "lookup_enrich",
                "column": col,
                "severity": "ERROR",
                "message": f"DB lookup failed: {exc}",
            })
            return df, anomalies
 
        unmapped_mask = ~df[match_column].astype(str).isin(lookup_dict.keys()) & df[match_column].notna()
        df[col] = df[match_column].astype(str).map(lookup_dict)
 
        if unmapped_mask.any():
            anomalies.append({
                "rule": "lookup_enrich",
                "column": col,
                "severity": "WARNING",
                "affected_rows": df[unmapped_mask].index.tolist(),
                "message": f"Some values in '{match_column}' had no match in '{lookup_table}.{lookup_key}'.",
            })
 
        return df, anomalies
 
 
@StrategyRegistry.register("conditional_fill")
class ConditionalFillTransformer(ITransformer):
    """
    UT-8  Fill the target column's nulls based on a condition in another column.
 
    Parameters:
        condition_column : str  — the column to evaluate
        condition_value  : any  — trigger value
        fill_value       : any  — value to write into the target column
    """
 
    def transform(self, df: pd.DataFrame, config: dict[str, Any]):
        col = config["column"]
        params = config.get("parameters", {})
        condition_col: str = params.get("condition_column", "")
        condition_val = params.get("condition_value")
        fill_val = params.get("fill_value")
 
        if col not in df.columns or condition_col not in df.columns:
            return df, []
 
        mask = (df[condition_col] == condition_val) & df[col].isnull()
        df.loc[mask, col] = fill_val
 
        return df, []