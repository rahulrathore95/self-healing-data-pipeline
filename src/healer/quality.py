"""Data-quality checks. Each check returns a list of Issue objects."""
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from .config import PipelineConfig


@dataclass
class Issue:
    code: str                      # machine-readable, e.g. "MISSING_COLUMN"
    detail: str                    # human-readable explanation
    column: str | None = None
    metric: float | None = None


def check_schema(df: pd.DataFrame, required_columns) -> list[Issue]:
    return [
        Issue("MISSING_COLUMN", f"required column '{col}' is missing from the extract", column=col)
        for col in required_columns
        if col not in df.columns
    ]


def check_types(df: pd.DataFrame) -> list[Issue]:
    issues = []
    if "amount" in df.columns:
        coerced = pd.to_numeric(df["amount"], errors="coerce")
        bad = int((coerced.isna() & df["amount"].notna()).sum())
        if bad:
            issues.append(
                Issue(
                    "TYPE_MISMATCH",
                    f"{bad} values in 'amount' are not numeric",
                    column="amount",
                    metric=bad / max(len(df), 1),
                )
            )
    return issues


def check_nulls(df: pd.DataFrame, required_columns, max_rate: float) -> list[Issue]:
    issues = []
    n = max(len(df), 1)
    for col in required_columns:
        if col not in df.columns:
            continue  # schema check already reported this
        rate = float(df[col].isna().sum()) / n
        if rate > max_rate:
            issues.append(
                Issue(
                    "NULL_SPIKE",
                    f"column '{col}' null rate {rate:.1%} exceeds the {max_rate:.1%} threshold",
                    column=col,
                    metric=rate,
                )
            )
    return issues


def check_duplicates(df: pd.DataFrame, primary_key: str) -> list[Issue]:
    if primary_key not in df.columns:
        return []
    dup_rate = float(df.duplicated(subset=[primary_key]).sum()) / max(len(df), 1)
    if dup_rate > 0:
        return [
            Issue(
                "DUPLICATE_KEYS",
                f"primary key '{primary_key}' has a duplicate rate of {dup_rate:.1%}",
                column=primary_key,
                metric=dup_rate,
            )
        ]
    return []


def check_volume(row_count: int, expected_rows: int, min_ratio: float, max_ratio: float) -> list[Issue]:
    if expected_rows <= 0:
        return []
    ratio = row_count / expected_rows
    if ratio < min_ratio:
        return [Issue("VOLUME_DROP", f"row count {row_count} is {ratio:.0%} of the expected {expected_rows}", metric=ratio)]
    if ratio > max_ratio:
        return [Issue("VOLUME_SPIKE", f"row count {row_count} is {ratio:.0%} of the expected {expected_rows}", metric=ratio)]
    return []


def run_all_checks(df: pd.DataFrame, config: PipelineConfig) -> list[Issue]:
    issues: list[Issue] = []
    issues += check_schema(df, config.required_columns)
    issues += check_types(df)
    issues += check_nulls(df, config.required_columns, config.thresholds.max_null_rate)
    issues += check_duplicates(df, config.primary_key)
    issues += check_volume(
        len(df),
        config.expected_daily_rows,
        config.thresholds.min_row_count_ratio,
        config.thresholds.max_row_count_ratio,
    )
    return issues
