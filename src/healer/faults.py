"""Fault injectors: the failures the demo and tests heal from.

Each injector simulates a classic upstream breakage. They are deterministic
so every demo run and test sees the same failure.
"""
from __future__ import annotations

import pandas as pd


def inject_schema_drift(df: pd.DataFrame) -> pd.DataFrame:
    """Upstream renamed a column without telling anyone."""
    return df.rename(columns={"amount": "order_amount"})


def inject_type_corruption(df: pd.DataFrame) -> pd.DataFrame:
    """Upstream started serializing amounts as currency-prefixed strings."""
    out = df.copy()
    out["amount"] = out["amount"].map(lambda v: f"USD {v}")
    return out


def inject_null_spike(df: pd.DataFrame, rate: float = 0.2) -> pd.DataFrame:
    """A broken upstream join leaves a fifth of rows without a customer."""
    out = df.copy()
    idx = out.sample(frac=rate, random_state=7).index
    out.loc[idx, "customer_id"] = None
    return out


def inject_duplicates(df: pd.DataFrame, rate: float = 0.05) -> pd.DataFrame:
    """A retried upstream extract re-delivers part of the batch."""
    dupes = df.sample(frac=rate, random_state=11)
    return pd.concat([df, dupes], ignore_index=True)


def inject_volume_drop(df: pd.DataFrame, keep: float = 0.1) -> pd.DataFrame:
    """A partial extract: only a slice of the day's data arrived."""
    return df.sample(frac=keep, random_state=13).reset_index(drop=True)
