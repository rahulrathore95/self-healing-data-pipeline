"""Root-cause diagnosis.

Rule-based by default and deterministic. If HEALER_LLM_* env vars are set,
an LLM advisor is consulted only when the rules cannot classify a failure
(see llm.py). Rules always win when they fire.
"""
from __future__ import annotations

from dataclasses import dataclass

from .quality import Issue


@dataclass
class Diagnosis:
    fault_type: str   # schema_drift | type_corruption | null_spike | duplicate_keys | volume_drop | unknown
    confidence: float
    rationale: str


def diagnose(issues: list[Issue]) -> Diagnosis:
    codes = {i.code for i in issues}
    if "MISSING_COLUMN" in codes:
        missing = [i.column for i in issues if i.code == "MISSING_COLUMN"]
        return Diagnosis(
            "schema_drift", 0.95,
            f"required column(s) {missing} missing; upstream likely renamed or dropped fields",
        )
    if "TYPE_MISMATCH" in codes:
        cols = [i.column for i in issues if i.code == "TYPE_MISMATCH"]
        return Diagnosis(
            "type_corruption", 0.9,
            f"non-numeric values in {cols}; upstream likely changed serialization "
            "(e.g. currency-prefixed strings)",
        )
    if "DUPLICATE_KEYS" in codes:
        return Diagnosis(
            "duplicate_keys", 0.9,
            "duplicate primary keys; likely a retried upstream extract or a join fan-out",
        )
    if "NULL_SPIKE" in codes:
        cols = [i.column for i in issues if i.code == "NULL_SPIKE"]
        return Diagnosis(
            "null_spike", 0.85,
            f"null rate above threshold in {cols}; likely a broken upstream join or feed gap",
        )
    if "VOLUME_DROP" in codes:
        return Diagnosis(
            "volume_drop", 0.8,
            "row count far below baseline; likely a partial extract or an upstream outage",
        )
    return Diagnosis("unknown", 0.0, "no rule matched; escalate or consult the LLM advisor")
