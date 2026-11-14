"""Configuration for the self-healing pipeline."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Thresholds:
    """Data-quality thresholds. Anything past these trips a check."""

    max_null_rate: float = 0.01        # per required column
    min_row_count_ratio: float = 0.5   # vs. expected baseline volume
    max_row_count_ratio: float = 3.0   # vs. expected baseline volume


@dataclass
class PipelineConfig:
    thresholds: Thresholds = field(default_factory=Thresholds)
    max_heal_attempts: int = 3         # circuit breaker: heals before escalating
    required_columns: tuple = ("order_id", "customer_id", "order_ts", "amount", "currency")
    primary_key: str = "order_id"
    expected_daily_rows: int = 500     # baseline volume for anomaly detection
    warehouse_path: Path = Path("demo_out/warehouse.db")
    reports_dir: Path = Path("demo_out/reports")
