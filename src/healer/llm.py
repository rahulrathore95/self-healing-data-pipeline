"""Optional LLM advisor for diagnosis.

The pipeline heals itself deterministically by default. If HEALER_LLM_API_KEY
is set (plus optional HEALER_LLM_BASE_URL and HEALER_LLM_MODEL), the healing
loop asks an OpenAI-compatible chat endpoint to classify failures the rules
cannot, and falls back to escalation on any error. Nothing here runs unless
the env var is set, so local demos and CI stay fully offline.
"""
from __future__ import annotations

import json
import os
import urllib.request

KNOWN_FAULTS = ["schema_drift", "type_corruption", "null_spike", "duplicate_keys", "volume_drop"]


def available() -> bool:
    return bool(os.environ.get("HEALER_LLM_API_KEY"))


def advise(issues) -> str | None:
    """Return a fault_type for issues the rules could not classify, or None."""
    if not available():
        return None
    base_url = os.environ.get("HEALER_LLM_BASE_URL", "https://api.openai.com/v1")
    model = os.environ.get("HEALER_LLM_MODEL", "gpt-4o-mini")
    prompt = (
        "You are diagnosing a failed data-pipeline run. "
        f"Pick exactly one fault type from {KNOWN_FAULTS} and reply with only that token.\n"
        "Issues:\n" + "\n".join(f"- {i.code}: {i.detail}" for i in issues)
    )
    body = json.dumps({
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0,
    }).encode()
    req = urllib.request.Request(
        f"{base_url.rstrip('/')}/chat/completions",
        data=body,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {os.environ['HEALER_LLM_API_KEY']}",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            payload = json.loads(resp.read().decode())
        text = payload["choices"][0]["message"]["content"].strip().lower()
        for fault in KNOWN_FAULTS:
            if fault in text:
                return fault
    except Exception:
        return None  # the advisor is best-effort; escalation is the safe fallback
    return None
