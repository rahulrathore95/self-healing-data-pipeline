from healer import faults
from healer.config import PipelineConfig
from healer.dataset import generate_orders
from healer.quality import run_all_checks


def codes(df, config):
    return {i.code for i in run_all_checks(df, config)}


def test_clean_data_passes_all_checks(tmp_path):
    config = PipelineConfig(warehouse_path=tmp_path / "wh.db", reports_dir=tmp_path / "r")
    assert codes(generate_orders(), config) == set()


def test_schema_drift_detected(tmp_path):
    config = PipelineConfig(warehouse_path=tmp_path / "wh.db", reports_dir=tmp_path / "r")
    assert "MISSING_COLUMN" in codes(faults.inject_schema_drift(generate_orders()), config)


def test_type_corruption_detected(tmp_path):
    config = PipelineConfig(warehouse_path=tmp_path / "wh.db", reports_dir=tmp_path / "r")
    assert "TYPE_MISMATCH" in codes(faults.inject_type_corruption(generate_orders()), config)


def test_null_spike_detected(tmp_path):
    config = PipelineConfig(warehouse_path=tmp_path / "wh.db", reports_dir=tmp_path / "r")
    assert "NULL_SPIKE" in codes(faults.inject_null_spike(generate_orders()), config)


def test_duplicates_detected(tmp_path):
    config = PipelineConfig(warehouse_path=tmp_path / "wh.db", reports_dir=tmp_path / "r")
    assert "DUPLICATE_KEYS" in codes(faults.inject_duplicates(generate_orders()), config)


def test_volume_drop_detected(tmp_path):
    config = PipelineConfig(warehouse_path=tmp_path / "wh.db", reports_dir=tmp_path / "r")
    assert "VOLUME_DROP" in codes(faults.inject_volume_drop(generate_orders()), config)
