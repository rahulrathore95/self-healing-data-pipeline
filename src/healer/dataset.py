"""Deterministic synthetic source data, so the demo and tests are repeatable."""
from __future__ import annotations

import random
from datetime import datetime, timedelta

import pandas as pd


def generate_orders(n: int = 500, seed: int = 42, day: str = "2026-11-14") -> pd.DataFrame:
    rng = random.Random(seed)
    base = datetime.fromisoformat(day)
    rows = []
    for i in range(n):
        rows.append(
            {
                "order_id": f"O{100000 + i}",
                "customer_id": f"C{rng.randint(1000, 1199)}",
                "order_ts": (base + timedelta(seconds=rng.randint(0, 86399))).isoformat(),
                "amount": round(rng.uniform(5.0, 500.0), 2),
                "currency": rng.choice(["INR", "USD", "EUR"]),
                "status": rng.choice(["placed", "shipped", "delivered", "cancelled"]),
            }
        )
    return pd.DataFrame(rows)
