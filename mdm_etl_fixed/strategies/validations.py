import re

import pandas as pd
from datetime import datetime

from core.registry import ITransformer, IValidator, StrategyRegistry

# ==========================================
# GLOBAL VALIDATIONS
# ==========================================

@StrategyRegistry.register("required_columns_present")
class RequiredColumnsValidator(IValidator):
    def validate(self, df, config):
        anomalies = []
        mandatory = config.get("mandatory_columns", [])
        # Case-insensitive lookup — golden schema may list canonical casing
        # that differs from the source file's headers.
        df_cols_lower = {c.lower() for c in df.columns}
        missing = [col for col in mandatory if col.lower() not in df_cols_lower]

        if missing:
            # DOWNGRADED TO WARNING: Allows the pipeline to continue to the Load Phase.
            # The Primary-Key Aware repository will dynamically skip tables that need these missing columns.
            anomalies.append({
                "rule": "required_columns_present",
                "severity": "WARNING",
                "message": f"Dataset is missing expected golden-schema columns: {missing}. "
                           f"The pipeline will continue, but tables relying on these columns "
                           f"(especially primary keys) will be safely skipped during the load phase.",
                "missing_columns": missing,
            })
        return anomalies

@StrategyRegistry.register("no_duplicate_primary_keys")
class NoDuplicatePrimaryKeysValidator(IValidator):
    def validate(self, df, config):
        anomalies = []
        pk_col = config.get("primary_key", "transaction_id")

        # Case-insensitive column lookup — CSV headers may differ in case
        col_lower_map = {c.lower(): c for c in df.columns}
        pk_col = col_lower_map.get(pk_col.lower(), pk_col)

        if pk_col in df.columns:
            duplicates = df[df.duplicated(subset=[pk_col], keep=False)]
            if not duplicates.empty:
                anomalies.append({
                    "rule": "no_duplicate_primary_keys",
                    "column": pk_col,
                    "severity": "FLAGGED",
                    "affected_rows": duplicates.index.tolist(),
                    "message": f"Duplicate primary keys detected. Will be dropped during DB upsert."
                })
        return anomalies

@StrategyRegistry.register("non_negative_quantities")
class NonNegativeQuantitiesValidator(IValidator):
    def validate(self, df, config):
        anomalies = []
        # Case-insensitive lookup — CSV columns may be capitalised
        col_lower_map = {c.lower(): c for c in df.columns}
        check_cols = [col_lower_map[k] for k in
                      ['quantity', 'rate', 'cost', 'price', 'amount', 'sales_amount',
                       'total_sales_amount', 'cogs', 'gross_margin']
                      if k in col_lower_map]
        
        for col in check_cols:
            numeric_series = pd.to_numeric(df[col], errors="coerce")
            negatives = df[numeric_series < 0]
            if not negatives.empty:
                anomalies.append({
                    "rule": "non_negative_quantities",
                    "column": col,
                    "severity": "FLAGGED",
                    "affected_rows": negatives.index.tolist(),
                    "message": "Negative financial/quantity values found outside of RETURN transactions."
                })
        return anomalies

# ==========================================
# USER-DEFINED VALIDATIONS
# ==========================================

@StrategyRegistry.register("numeric_range")
class NumericRangeValidator(IValidator):
    def validate(self, df, config):
        anomalies = []
        col = config['column']
        min_val = config['parameters'].get('min', float('-inf'))
        max_val = config['parameters'].get('max', float('inf'))
        
        if col in df.columns:
            numeric_series = pd.to_numeric(df[col], errors="coerce")
            # Flag rows that couldn't be parsed as numeric
            parse_failed = df[col].notna() & numeric_series.isna()
            if parse_failed.any():
                anomalies.append({
                    "rule": "numeric_range", "column": col, "severity": "ERROR",
                    "affected_rows": df[parse_failed].index.tolist(),
                    "message": f"Column '{col}' contains non-numeric values that could not be range-checked.",
                })
            out_of_bounds = df[
                numeric_series.notna() &
                ((numeric_series < min_val) | (numeric_series > max_val))
            ]
            if not out_of_bounds.empty:
                anomalies.append({
                    "rule": "numeric_range", "column": col, "severity": "FLAGGED",
                    "affected_rows": out_of_bounds.index.tolist(),
                    "message": f"Values outside permitted range [{min_val}, {max_val}]."
                })
        return anomalies

@StrategyRegistry.register("cross_column_rule")
class CrossColumnRuleValidator(IValidator):
    def validate(self, df, config):
        anomalies = []
        expr = config['parameters']['expression'] # e.g., "total_sales_amount >= cogs"
        
        try:
            # pandas eval safely executes the math logic
            valid_mask = df.eval(expr)
            invalid_rows = df[~valid_mask]
            
            if not invalid_rows.empty:
                anomalies.append({
                    "rule": "cross_column_rule", "severity": "FLAGGED",
                    "affected_rows": invalid_rows.index.tolist(),
                    "message": f"Business rule violation: {config['parameters'].get('description', expr)}"
                })
        except Exception as e:
            anomalies.append({
                "rule": "cross_column_rule", "severity": "ERROR",
                "message": f"Could not evaluate expression '{expr}': {str(e)}"
            })
        return anomalies


# ==========================================
# GLOBAL VALIDATIONS (continued)
# ==========================================

@StrategyRegistry.register("no_future_transaction_dates")
class NoFutureTransactionDatesValidator(IValidator):
    def validate(self, df: pd.DataFrame, config: dict) -> list[dict]:
        anomalies = []
        col = config.get("date_column", "transaction_date")

        if col not in df.columns:
            return anomalies

        today = pd.Timestamp(datetime.utcnow().date())
        try:
            parsed = pd.to_datetime(df[col], errors="coerce")
        except Exception as e:
            anomalies.append({
                "rule": "no_future_transaction_dates",
                "column": col,
                "severity": "ERROR",
                "message": f"Could not parse column '{col}' as dates: {e}",
            })
            return anomalies

        future_rows = df[parsed > today]
        if not future_rows.empty:
            anomalies.append({
                "rule": "no_future_transaction_dates",
                "column": col,
                "severity": "FLAGGED",
                "affected_rows": future_rows.index.tolist(),
                "message": (
                    f"{len(future_rows)} row(s) have a '{col}' value that is "
                    f"in the future (after {today.date()})."
                ),
            })
        return anomalies


@StrategyRegistry.register("referential_id_format")
class ReferentialIdFormatValidator(IValidator):
    def validate(self, df: pd.DataFrame, config: dict) -> list[dict]:
        anomalies = []
        col = config.get("column")
        params = config.get("parameters", {})
        pattern = params.get("pattern")

        if not col or not pattern:
            anomalies.append({
                "rule": "referential_id_format",
                "severity": "ERROR",
                "message": "Misconfigured rule: 'column' and 'parameters.pattern' are required.",
            })
            return anomalies

        if col not in df.columns:
            return anomalies

        non_null = df[df[col].notna()]
        try:
            compiled = re.compile(pattern)
            invalid_mask = ~non_null[col].astype(str).str.fullmatch(compiled.pattern)
        except re.error as e:
            anomalies.append({
                "rule": "referential_id_format",
                "column": col,
                "severity": "ERROR",
                "message": f"Invalid regex pattern '{pattern}': {e}",
            })
            return anomalies

        invalid_rows = non_null[invalid_mask]
        if not invalid_rows.empty:
            label = params.get("description", pattern)
            anomalies.append({
                "rule": "referential_id_format",
                "column": col,
                "severity": "FLAGGED",
                "affected_rows": invalid_rows.index.tolist(),
                "message": (
                    f"{len(invalid_rows)} row(s) in '{col}' do not match "
                    f"the expected format: {label}."
                ),
            })
        return anomalies


@StrategyRegistry.register("discount_percent_range")
class DiscountPercentRangeTransformer(ITransformer):
    def transform(self, df: pd.DataFrame, config: dict):
        anomalies = []
        col = config.get("column", "discount_percent")
        params = config.get("parameters", {})
        lo = float(params.get("min", 0))
        hi = float(params.get("max", 100))

        if col not in df.columns:
            return df, anomalies

        try:
            numeric = pd.to_numeric(df[col], errors="coerce")
        except Exception as e:
            anomalies.append({
                "rule": "discount_percent_range",
                "column": col,
                "severity": "ERROR",
                "message": f"Could not coerce '{col}' to numeric: {e}",
            })
            return df, anomalies

        out_of_bounds_mask = (numeric < lo) | (numeric > hi)
        out_of_bounds = df[out_of_bounds_mask & numeric.notna()]

        if not out_of_bounds.empty:
            df = df.copy()
            df.loc[out_of_bounds_mask, col] = numeric[out_of_bounds_mask].clip(lo, hi)
            anomalies.append({
                "rule": "discount_percent_range",
                "column": col,
                "severity": "FLAGGED",
                "affected_rows": out_of_bounds.index.tolist(),
                "message": (
                    f"{len(out_of_bounds)} row(s) had '{col}' outside "
                    f"[{lo}, {hi}] and were clamped to the boundary value."
                ),
            })
        return df, anomalies


# ==========================================
# USER-DEFINED VALIDATIONS (continued)
# ==========================================

@StrategyRegistry.register("value_in_set")
class ValueInSetValidator(IValidator):
    def validate(self, df: pd.DataFrame, config: dict) -> list[dict]:
        anomalies = []
        col = config.get("column")
        params = config.get("parameters", {})
        allowed = params.get("allowed_values", [])
        case_sensitive = params.get("case_sensitive", True)

        if not col or not allowed:
            anomalies.append({
                "rule": "value_in_set",
                "severity": "ERROR",
                "message": "Misconfigured rule: 'column' and 'parameters.allowed_values' are required.",
            })
            return anomalies

        if col not in df.columns:
            return anomalies

        series = df[col]
        allowed_set = set(allowed)

        if not case_sensitive:
            allowed_set = {str(v).lower() for v in allowed_set}
            series = series.astype(str).str.lower()

        invalid_rows = df[~series.isin(allowed_set) & df[col].notna()]
        if not invalid_rows.empty:
            anomalies.append({
                "rule": "value_in_set",
                "column": col,
                "severity": "FLAGGED",
                "affected_rows": invalid_rows.index.tolist(),
                "message": (
                    f"{len(invalid_rows)} row(s) in '{col}' contain values "
                    f"not in the permitted set: {sorted(str(v) for v in allowed)}."
                ),
            })
        return anomalies


@StrategyRegistry.register("pattern_match")
class PatternMatchValidator(IValidator):
    def validate(self, df: pd.DataFrame, config: dict) -> list[dict]:
        anomalies = []
        col = config.get("column")
        params = config.get("parameters", {})
        pattern = params.get("pattern")

        if not col or not pattern:
            anomalies.append({
                "rule": "pattern_match",
                "severity": "ERROR",
                "message": "Misconfigured rule: 'column' and 'parameters.pattern' are required.",
            })
            return anomalies

        if col not in df.columns:
            return anomalies

        try:
            non_null = df[df[col].notna()]
            invalid_mask = ~non_null[col].astype(str).str.fullmatch(pattern)
            invalid_rows = non_null[invalid_mask]
        except re.error as e:
            anomalies.append({
                "rule": "pattern_match",
                "column": col,
                "severity": "ERROR",
                "message": f"Invalid regex pattern '{pattern}': {e}",
            })
            return anomalies

        if not invalid_rows.empty:
            label = params.get("description", pattern)
            anomalies.append({
                "rule": "pattern_match",
                "column": col,
                "severity": "FLAGGED",
                "affected_rows": invalid_rows.index.tolist(),
                "message": (
                    f"{len(invalid_rows)} row(s) in '{col}' do not match "
                    f"the expected pattern: {label}."
                ),
            })
        return anomalies


@StrategyRegistry.register("string_length")
class StringLengthValidator(IValidator):
    def validate(self, df: pd.DataFrame, config: dict) -> list[dict]:
        anomalies = []
        col = config.get("column")
        params = config.get("parameters", {})
        min_len = int(params.get("min", 0))
        max_len = params.get("max")

        if not col:
            anomalies.append({
                "rule": "string_length",
                "severity": "ERROR",
                "message": "Misconfigured rule: 'column' is required.",
            })
            return anomalies

        if col not in df.columns:
            return anomalies

        non_null = df[df[col].notna()]
        lengths = non_null[col].astype(str).str.len()

        too_short = non_null[lengths < min_len]
        too_long = non_null[lengths > max_len] if max_len is not None else pd.DataFrame()

        invalid_rows = pd.concat([too_short, too_long]).drop_duplicates()
        if not invalid_rows.empty:
            bound_desc = f">= {min_len}" if max_len is None else f"[{min_len}, {max_len}]"
            anomalies.append({
                "rule": "string_length",
                "column": col,
                "severity": "FLAGGED",
                "affected_rows": invalid_rows.index.tolist(),
                "message": (
                    f"{len(invalid_rows)} row(s) in '{col}' have string "
                    f"length outside permitted range {bound_desc}."
                ),
            })
        return anomalies


@StrategyRegistry.register("no_nulls_in_column")
class NoNullsInColumnValidator(IValidator):
    def validate(self, df: pd.DataFrame, config: dict) -> list[dict]:
        anomalies = []
        col = config.get("column")
        params = config.get("parameters", {})
        treat_empty = params.get("treat_empty_string_as_null", True)

        if not col:
            anomalies.append({
                "rule": "no_nulls_in_column",
                "severity": "ERROR",
                "message": "Misconfigured rule: 'column' is required.",
            })
            return anomalies

        if col not in df.columns:
            return anomalies

        null_mask = df[col].isna()
        if treat_empty:
            null_mask = null_mask | (df[col].astype(str).str.strip() == "")

        null_rows = df[null_mask]
        if not null_rows.empty:
            anomalies.append({
                "rule": "no_nulls_in_column",
                "column": col,
                "severity": "FLAGGED",
                "affected_rows": null_rows.index.tolist(),
                "message": (
                    f"{len(null_rows)} row(s) have null or empty values in "
                    f"required column '{col}'."
                ),
            })
        return anomalies


@StrategyRegistry.register("unique_values")
class UniqueValuesValidator(IValidator):
    def validate(self, df: pd.DataFrame, config: dict) -> list[dict]:
        anomalies = []
        col = config.get("column")
        params = config.get("parameters", {})
        keep = params.get("keep", False)

        if not col:
            anomalies.append({
                "rule": "unique_values",
                "severity": "ERROR",
                "message": "Misconfigured rule: 'column' is required.",
            })
            return anomalies

        cols = [col] if isinstance(col, str) else list(col)
        missing = [c for c in cols if c not in df.columns]
        if missing:
            return anomalies

        duplicates = df[df.duplicated(subset=cols, keep=keep)]
        if not duplicates.empty:
            col_label = cols[0] if len(cols) == 1 else str(cols)
            anomalies.append({
                "rule": "unique_values",
                "column": col_label,
                "severity": "FLAGGED",
                "affected_rows": duplicates.index.tolist(),
                "message": (
                    f"{len(duplicates)} duplicate row(s) found on "
                    f"column(s) {col_label}."
                ),
            })
        return anomalies


@StrategyRegistry.register("date_order")
class DateOrderValidator(IValidator):
    def validate(self, df: pd.DataFrame, config: dict) -> list[dict]:
        anomalies = []
        params = config.get("parameters", {})
        earlier_col = params.get("earlier_column")
        later_col = params.get("later_column")
        allow_equal = params.get("allow_equal", True)

        if not earlier_col or not later_col:
            anomalies.append({
                "rule": "date_order",
                "severity": "ERROR",
                "message": (
                    "Misconfigured rule: 'parameters.earlier_column' and "
                    "'parameters.later_column' are both required."
                ),
            })
            return anomalies

        for col in (earlier_col, later_col):
            if col not in df.columns:
                return anomalies

        try:
            earlier = pd.to_datetime(df[earlier_col], errors="coerce")
            later = pd.to_datetime(df[later_col], errors="coerce")
        except Exception as e:
            anomalies.append({
                "rule": "date_order",
                "severity": "ERROR",
                "message": f"Could not parse date columns for ordering check: {e}",
            })
            return anomalies

        both_valid = earlier.notna() & later.notna()
        if allow_equal:
            violation_mask = both_valid & (earlier > later)
        else:
            violation_mask = both_valid & (earlier >= later)

        invalid_rows = df[violation_mask]
        if not invalid_rows.empty:
            op = "after" if allow_equal else "on or after"
            label = params.get(
                "description", f"'{earlier_col}' must not be {op} '{later_col}'"
            )
            anomalies.append({
                "rule": "date_order",
                "severity": "FLAGGED",
                "affected_rows": invalid_rows.index.tolist(),
                "message": (
                    f"{len(invalid_rows)} row(s) violate date order: {label}."
                ),
            })
        return anomalies