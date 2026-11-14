from healer.config import PipelineConfig
from healer.demo import run_demo


def test_full_demo_all_scenarios_heal(tmp_path):
    config = PipelineConfig(warehouse_path=tmp_path / "demo" / "wh.db",
                            reports_dir=tmp_path / "demo" / "reports")
    result = run_demo(config)
    assert result["healed"] == 5
    assert all(s["status"] == "success" for s in result["summary"])
    assert (tmp_path / "demo" / "reports" / "last_run.json").exists()
    assert (tmp_path / "demo" / "reports" / "incidents.jsonl").exists()
