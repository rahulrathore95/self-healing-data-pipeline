import json

import pytest

from healer import faults
from healer.config import PipelineConfig
from healer.dataset import generate_orders
from healer.heal import SelfHealingRunner
from healer.pipeline import OrdersPipeline


def make_pipeline(tmp_path):
    config = PipelineConfig(warehouse_path=tmp_path / "wh.db", reports_dir=tmp_path / "reports")
    return OrdersPipeline(config)


@pytest.mark.parametrize(
    "injector, expected_loaded, expected_quarantined",
    [
        (faults.inject_schema_drift, 500, 0),
        (faults.inject_type_corruption, 500, 0),
        (faults.inject_null_spike, 400, 100),   # 20% quarantined
        (faults.inject_duplicates, 500, 0),
        (faults.inject_volume_drop, 500, 0),
    ],
)
def test_each_fault_is_healed(tmp_path, injector, expected_loaded, expected_quarantined):
    pipeline = make_pipeline(tmp_path)
    source = generate_orders()
    runner = SelfHealingRunner(pipeline, source)
    result = runner.run(injector(source))
    assert result.status == "success"
    assert result.attempts == 2            # one failure, one heal, one clean pass
    assert pipeline.warehouse_count() == expected_loaded
    assert pipeline.quarantined_count() == expected_quarantined
    incidents = (tmp_path / "reports" / "incidents.jsonl").read_text().strip().splitlines()
    assert len(incidents) == 1
    entry = json.loads(incidents[0])
    assert entry["outcome"] == "healing_attempted"
    assert entry["action"].startswith("playbook:")


def test_circuit_breaker_escalates_when_source_is_also_bad(tmp_path):
    pipeline = make_pipeline(tmp_path)
    bad_source = generate_orders().head(100)   # source genuinely lost data
    runner = SelfHealingRunner(pipeline, bad_source)
    result = runner.run(faults.inject_volume_drop(bad_source, keep=0.1))
    assert result.status == "escalated"
    assert result.attempts == pipeline.config.max_heal_attempts + 1
    lines = (tmp_path / "reports" / "incidents.jsonl").read_text().strip().splitlines()
    assert json.loads(lines[-1])["outcome"] == "ESCALATED"


def test_load_is_idempotent(tmp_path):
    pipeline = make_pipeline(tmp_path)
    source = generate_orders()
    SelfHealingRunner(pipeline, source).run(source)
    SelfHealingRunner(pipeline, source).run(source)  # re-run the same batch
    assert pipeline.warehouse_count() == 500
