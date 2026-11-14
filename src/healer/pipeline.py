"""The ETL pipeline: validate -> transform -> load.

A run validates first and refuses to load bad data (fail-fast). That clean
failure signal is what the self-healing loop works from.
"""
from __future__ import annotations

import sqlite3
import time
import uuid
from dataclasses import dataclass, field

import pandas as pd

from .config import PipelineConfig
from .quality import Issue, run_all_checks

DDL = """
CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    order_ts TEXT NOT NULL,
    amount REAL NOT NULL,
    currency TEXT NOT NULL,
    status TEXT,
    loaded_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS orders_quarantine (
    order_id TEXT, customer_id TEXT, order_ts TEXT, amount TEXT, currency TEXT,
    status TEXT, reason TEXT, quarantined_at TEXT NOT NULL
);
"""


@dataclass
class RunReport:
    run_id: str
    status: str                    # "success" | "failed"
    attempt: int
    rows_in: int = 0
    rows_loaded: int = 0
    issues: list[Issue] = field(default_factory=list)
    elapsed_s: float = 0.0

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "status": self.status,
            "attempt": self.attempt,
            "rows_in": self.rows_in,
            "rows_loaded": self.rows_loaded,
            "issues": [vars(i) for i in self.issues],
            "elapsed_s": round(self.elapsed_s, 3),
        }


class OrdersPipeline:
    """Loads an orders extract into a SQLite warehouse, idempotently."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        config.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
        config.reports_dir.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(config.warehouse_path))
        self.conn.executescript(DDL)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out["amount"] = pd.to_numeric(out["amount"], errors="coerce")
        out["order_ts"] = out["order_ts"].astype(str)
        return out

    def load(self, df: pd.DataFrame) -> int:
        """Idempotent upsert keyed on the primary key; safe to re-run."""
        now = pd.Timestamp.utcnow().isoformat()
        rows = [
            (r.order_id, r.customer_id, r.order_ts, float(r.amount), r.currency, getattr(r, "status", None), now)
            for r in df.itertuples(index=False)
        ]
        self.conn.executemany(
            """INSERT INTO orders (order_id, customer_id, order_ts, amount, currency, status, loaded_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(order_id) DO UPDATE SET
                   customer_id = excluded.customer_id,
                   order_ts    = excluded.order_ts,
                   amount      = excluded.amount,
                   currency    = excluded.currency,
                   status      = excluded.status,
                   loaded_at   = excluded.loaded_at""",
            rows,
        )
        self.conn.commit()
        return len(rows)

    def quarantine(self, df: pd.DataFrame, reason: str) -> int:
        """Park bad rows in a side table instead of dropping or loading them."""
        if df.empty:
            return 0
        now = pd.Timestamp.utcnow().isoformat()
        cols = ["order_id", "customer_id", "order_ts", "amount", "currency", "status"]
        tmp = df.reindex(columns=cols)
        rows = [
            tuple(None if pd.isna(v) else str(v) for v in row) + (reason, now)
            for row in tmp.itertuples(index=False, name=None)
        ]
        self.conn.executemany(
            """INSERT INTO orders_quarantine
               (order_id, customer_id, order_ts, amount, currency, status, reason, quarantined_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        self.conn.commit()
        return len(rows)

    def run_once(self, df: pd.DataFrame, attempt: int = 1) -> RunReport:
        start = time.monotonic()
        run_id = uuid.uuid4().hex[:12]
        issues = run_all_checks(df, self.config)
        if issues:
            return RunReport(run_id, "failed", attempt, rows_in=len(df), issues=issues,
                             elapsed_s=time.monotonic() - start)
        loaded = self.load(self.transform(df))
        return RunReport(run_id, "success", attempt, rows_in=len(df), rows_loaded=loaded,
                         elapsed_s=time.monotonic() - start)

    def warehouse_count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]

    def quarantined_count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM orders_quarantine").fetchone()[0]
