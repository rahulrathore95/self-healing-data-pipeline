"""Runnable demo: inject five classic failures, watch the pipeline heal itself.

Run with:  PYTHONPATH=src python -m healer.demo
"""
from __future__ import annotations

import json
import shutil
from pathlib import Path

from .config import PipelineConfig
from .dataset import generate_orders
from . import faults
from .heal import PLAYBOOKS, SelfHealingRunner
from .pipeline import OrdersPipeline

SCENARIOS = [
    ("schema drift", "upstream renamed 'amount' to 'order_amount'", faults.inject_schema_drift),
    ("type corruption", "amounts arrived as 'USD 12.30' strings", faults.inject_type_corruption),
    ("null spike", "a broken upstream join dropped customer_id on 20% of rows", faults.inject_null_spike),
    ("duplicate keys", "a retried extract re-delivered 5% of the batch", faults.inject_duplicates),
    ("volume drop", "a partial extract delivered only 10% of the day", faults.inject_volume_drop),
]


def run_scenario(runner: SelfHealingRunner, df, label: str = "") -> dict:
    result = runner.run(df)
    for report in result.history:
        if report.status == "failed":
            print(f"  attempt {report.attempt}: FAILED")
            for issue in report.issues:
                print(f"      {issue.code}: {issue.detail}")
            d = result.diagnoses[report.attempt - 1]
            print(f"  diagnose: {d.fault_type} (confidence {d.confidence:.2f}) - {d.rationale}")
            if d.fault_type in PLAYBOOKS and result.status != "escalated":
                print(f"  heal: applying playbook '{d.fault_type}'")
        else:
            print(f"  attempt {report.attempt}: SUCCESS - loaded {report.rows_loaded} rows")
    if result.status == "escalated":
        print(f"  ESCALATED after {result.attempts} attempts - circuit breaker open, paging a human")
    return {"scenario": label, "status": result.status, "attempts": result.attempts}


def run_demo(config: PipelineConfig, clean: bool = False) -> dict:
    if clean and config.warehouse_path.parent.exists():
        shutil.rmtree(config.warehouse_path.parent)
    pipeline = OrdersPipeline(config)
    source = generate_orders(n=config.expected_daily_rows)
    print("=== Self-healing data pipeline demo ===")
    print(f"[setup] generated {len(source)} synthetic orders")
    print(f"[setup] warehouse: {config.warehouse_path}")
    print(f"[setup] incident log: {config.reports_dir / 'incidents.jsonl'}")

    summary = []
    print("\n--- Scenario 0: clean run (baseline) ---")
    runner = SelfHealingRunner(pipeline, source)
    summary.append(run_scenario(runner, source, "clean run"))

    for i, (name, blurb, injector) in enumerate(SCENARIOS, start=1):
        print(f"\n--- Scenario {i}: {name} ({blurb}) ---")
        runner = SelfHealingRunner(pipeline, source)
        summary.append(run_scenario(runner, injector(source), name))

    report_path = config.reports_dir / "last_run.json"
    with open(report_path, "w") as fh:
        json.dump({"scenarios": summary}, fh, indent=2)

    healed = sum(1 for s in summary[1:] if s["status"] == "success")
    print("\n=== Summary ===")
    print(f"{healed}/{len(SCENARIOS)} injected failures healed automatically")
    print(f"warehouse rows: {pipeline.warehouse_count()} | quarantined: {pipeline.quarantined_count()}")
    print(f"incidents + run report written to {config.reports_dir}")
    return {"summary": summary, "healed": healed}


def main() -> int:
    config = PipelineConfig(
        warehouse_path=Path("demo_out/warehouse.db"),
        reports_dir=Path("demo_out/reports"),
    )
    run_demo(config, clean=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
