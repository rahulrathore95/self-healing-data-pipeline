"""Incident log: every detected failure, diagnosis and action, as JSONL."""
from __future__ import annotations

import json

from .config import PipelineConfig
from .diagnose import Diagnosis
from .pipeline import RunReport


def record_incident(
    config: PipelineConfig,
    report: RunReport,
    diagnosis: Diagnosis,
    action: str,
    outcome: str,
) -> dict:
    config.reports_dir.mkdir(parents=True, exist_ok=True)
    entry = {
        "ts": __import__("datetime").datetime.utcnow().isoformat() + "Z",
        "run_id": report.run_id,
        "attempt": report.attempt,
        "issues": [vars(i) for i in report.issues],
        "diagnosis": vars(diagnosis),
        "action": action,
        "outcome": outcome,
    }
    with open(config.reports_dir / "incidents.jsonl", "a") as fh:
        fh.write(json.dumps(entry) + "\n")
    return entry
