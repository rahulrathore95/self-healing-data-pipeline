"""Remediation playbooks and the self-healing loop.

The loop: run -> on failure, diagnose -> apply the matching playbook ->
re-run. A circuit breaker caps the number of healing attempts; when it
opens, the incident is escalated instead of looping forever.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

import pandas as pd

from . import llm
from .diagnose import Diagnosis, diagnose
from .incident import record_incident
from .pipeline import OrdersPipeline, RunReport


@dataclass
class HealContext:
    pipeline: OrdersPipeline
    source_df: pd.DataFrame   # the source system, for re-extraction
    df: pd.DataFrame          # the current (broken) batch
    diagnosis: Diagnosis


@dataclass
class HealingResult:
    status: str                        # "success" | "escalated"
    attempts: int
    history: list[RunReport] = field(default_factory=list)
    diagnoses: list[Diagnosis] = field(default_factory=list)


def _norm(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())


def heal_schema_drift(ctx: HealContext) -> pd.DataFrame:
    """Map missing required columns onto present columns by normalized name."""
    df = ctx.df
    present = {_norm(c): c for c in df.columns}
    rename = {}
    for col in ctx.pipeline.config.required_columns:
        if col in df.columns:
            continue
        for ncol, orig in present.items():
            if _norm(col) in ncol or ncol in _norm(col):
                rename[orig] = col
                break
    return df.rename(columns=rename)


def heal_type_corruption(ctx: HealContext) -> pd.DataFrame:
    """Strip currency symbols / whitespace so amounts parse as numbers."""
    df = ctx.df.copy()
    df["amount"] = df["amount"].astype(str).str.replace(r"[^0-9.\-]", "", regex=True)
    return df


def heal_null_spike(ctx: HealContext) -> pd.DataFrame:
    """Quarantine rows missing required fields; load the valid remainder."""
    df = ctx.df
    required = [c for c in ctx.pipeline.config.required_columns if c in df.columns]
    bad = df[df[required].isna().any(axis=1)]
    ctx.pipeline.quarantine(bad, reason="null_spike")
    return df.dropna(subset=required)


def heal_duplicates(ctx: HealContext) -> pd.DataFrame:
    """De-duplicate on the primary key, keeping the latest occurrence."""
    return ctx.df.drop_duplicates(subset=[ctx.pipeline.config.primary_key], keep="last")


def heal_volume_drop(ctx: HealContext) -> pd.DataFrame:
    """Re-extract the full batch from the source system."""
    return ctx.source_df.copy()


PLAYBOOKS = {
    "schema_drift": heal_schema_drift,
    "type_corruption": heal_type_corruption,
    "null_spike": heal_null_spike,
    "duplicate_keys": heal_duplicates,
    "volume_drop": heal_volume_drop,
}


class SelfHealingRunner:
    """Wraps a pipeline run with the detect -> diagnose -> heal loop."""

    def __init__(self, pipeline: OrdersPipeline, source_df: pd.DataFrame, max_heal_attempts: int | None = None):
        self.pipeline = pipeline
        self.config = pipeline.config
        self.source_df = source_df
        self.max_heal_attempts = max_heal_attempts or self.config.max_heal_attempts

    def run(self, df: pd.DataFrame) -> HealingResult:
        attempt = 1
        current = df
        history: list[RunReport] = []
        diagnoses: list[Diagnosis] = []
        while True:
            report = self.pipeline.run_once(current, attempt=attempt)
            history.append(report)
            if report.status == "success":
                return HealingResult("success", attempt, history, diagnoses)

            diagnosis = diagnose(report.issues)
            if diagnosis.fault_type == "unknown" and llm.available():
                advised = llm.advise(report.issues)
                if advised in PLAYBOOKS:
                    diagnosis = Diagnosis(advised, 0.6, "LLM advisor classified this failure (rules did not)")
            diagnoses.append(diagnosis)
            playbook = PLAYBOOKS.get(diagnosis.fault_type)

            if playbook is None or attempt > self.max_heal_attempts:
                record_incident(self.config, report, diagnosis, action="none", outcome="ESCALATED")
                return HealingResult("escalated", attempt, history, diagnoses)

            record_incident(self.config, report, diagnosis,
                            action=f"playbook:{diagnosis.fault_type}", outcome="healing_attempted")
            ctx = HealContext(self.pipeline, self.source_df, current, diagnosis)
            current = playbook(ctx)
            attempt += 1
