from healer import faults
from healer.config import PipelineConfig
from healer.dataset import generate_orders
from healer.diagnose import diagnose
from healer.quality import run_all_checks


def diagnosis_for(injector, tmp_path):
    config = PipelineConfig(warehouse_path=tmp_path / "wh.db", reports_dir=tmp_path / "r")
    issues = run_all_checks(injector(generate_orders()), config)
    return diagnose(issues)


def test_schema_drift_diagnosed(tmp_path):
    assert diagnosis_for(faults.inject_schema_drift, tmp_path).fault_type == "schema_drift"


def test_type_corruption_diagnosed(tmp_path):
    assert diagnosis_for(faults.inject_type_corruption, tmp_path).fault_type == "type_corruption"


def test_null_spike_diagnosed(tmp_path):
    assert diagnosis_for(faults.inject_null_spike, tmp_path).fault_type == "null_spike"


def test_duplicates_diagnosed(tmp_path):
    assert diagnosis_for(faults.inject_duplicates, tmp_path).fault_type == "duplicate_keys"


def test_volume_drop_diagnosed(tmp_path):
    assert diagnosis_for(faults.inject_volume_drop, tmp_path).fault_type == "volume_drop"
