"""
tests/unit/test_pipeline_status.py

Unit tests for load_pipeline_status() business logic from app/data.py.
Pure-Python replica — no live Databricks SQL connection required.

Author: Gresa Hasani — MLOps / AI Ops Engineer
"""

import pytest


def _derive_pipeline_status(pq: dict, ss: dict) -> dict:
    dq_fail  = int(pq.get("dq_failed_rows")        or 0)
    dq_pass  = int(pq.get("dq_pass_rows")           or 0)
    pii_bad  = int(pq.get("pii_inconsistent_rows")  or 0)
    outliers = int(ss.get("outlier_tables")          or 0)
    total    = dq_pass + dq_fail

    steps = [
        {"name": "Bronze ingestion",       "status": "ok",
         "detail": f"{total:,} rows ingested from Azure Blob"},
        {"name": "Silver validation",      "status": "warn" if dq_fail > 0 else "ok",
         "detail": f"{dq_fail} rows exceed DQ threshold · routed to quarantine"
                   if dq_fail > 0 else f"All {total:,} rows pass DQ threshold"},
        {"name": "Gold scoring",           "status": "ok",
         "detail": "11 rubric checks applied · Gold tables written"},
        {"name": "KPI + gap tables",       "status": "ok",
         "detail": "kpi_summary · governance_gaps · dlt_summary refreshed"},
        {"name": "Structural consistency", "status": "warn" if outliers > 0 else "ok",
         "detail": f"{outliers} structural outlier(s) detected"
                   if outliers > 0 else "All tables match structural standard"},
        {"name": "Sensitivity check",      "status": "warn" if pii_bad > 0 else "ok",
         "detail": f"{pii_bad} PII columns with inconsistent flags"
                   if pii_bad > 0 else "All PII flags consistent"},
    ]
    return {
        "steps":           steps,
        "quarantine_rows": dq_fail,
        "total_rows":      total,
        "alerts_fired":    sum([dq_fail > 0, pii_bad > 0, outliers > 0]),
    }


@pytest.fixture
def clean():
    return (
        {"dq_pass_rows": 7100, "dq_failed_rows": 0,
         "pii_consistent_rows": 7100, "pii_inconsistent_rows": 0},
        {"outlier_tables": 0, "structural_consistency_pct": 100.0},
    )

@pytest.fixture
def dirty():
    return (
        {"dq_pass_rows": 6500, "dq_failed_rows": 600,
         "pii_consistent_rows": 6800, "pii_inconsistent_rows": 300},
        {"outlier_tables": 3, "structural_consistency_pct": 96.8},
    )


class TestCleanPipeline:

    def test_zero_alerts(self, clean):
        pq, ss = clean
        assert _derive_pipeline_status(pq, ss)["alerts_fired"] == 0

    def test_total_rows(self, clean):
        pq, ss = clean
        assert _derive_pipeline_status(pq, ss)["total_rows"] == 7100

    def test_zero_quarantine(self, clean):
        pq, ss = clean
        assert _derive_pipeline_status(pq, ss)["quarantine_rows"] == 0

    def test_six_steps(self, clean):
        pq, ss = clean
        assert len(_derive_pipeline_status(pq, ss)["steps"]) == 6

    def test_all_steps_ok(self, clean):
        pq, ss = clean
        for step in _derive_pipeline_status(pq, ss)["steps"]:
            assert step["status"] == "ok", f"Expected ok for {step['name']}"

    def test_silver_detail_all_pass(self, clean):
        pq, ss = clean
        result = _derive_pipeline_status(pq, ss)
        silver = next(s for s in result["steps"] if s["name"] == "Silver validation")
        assert "All" in silver["detail"]

    def test_bronze_shows_total(self, clean):
        pq, ss = clean
        result = _derive_pipeline_status(pq, ss)
        bronze = next(s for s in result["steps"] if s["name"] == "Bronze ingestion")
        assert "7,100" in bronze["detail"]


class TestDirtyPipeline:

    def test_three_alerts(self, dirty):
        pq, ss = dirty
        assert _derive_pipeline_status(pq, ss)["alerts_fired"] == 3

    def test_quarantine_rows(self, dirty):
        pq, ss = dirty
        assert _derive_pipeline_status(pq, ss)["quarantine_rows"] == 600

    def test_silver_warn(self, dirty):
        pq, ss = dirty
        result = _derive_pipeline_status(pq, ss)
        silver = next(s for s in result["steps"] if s["name"] == "Silver validation")
        assert silver["status"] == "warn"
        assert "600" in silver["detail"]
        assert "quarantine" in silver["detail"]

    def test_structural_warn(self, dirty):
        pq, ss = dirty
        result = _derive_pipeline_status(pq, ss)
        struct = next(s for s in result["steps"] if s["name"] == "Structural consistency")
        assert struct["status"] == "warn"
        assert "3" in struct["detail"]

    def test_sensitivity_warn(self, dirty):
        pq, ss = dirty
        result = _derive_pipeline_status(pq, ss)
        sens = next(s for s in result["steps"] if s["name"] == "Sensitivity check")
        assert sens["status"] == "warn"
        assert "300" in sens["detail"]

    def test_gold_always_ok(self, dirty):
        pq, ss = dirty
        result = _derive_pipeline_status(pq, ss)
        gold = next(s for s in result["steps"] if s["name"] == "Gold scoring")
        assert gold["status"] == "ok"

    def test_kpi_always_ok(self, dirty):
        pq, ss = dirty
        result = _derive_pipeline_status(pq, ss)
        kpi = next(s for s in result["steps"] if s["name"] == "KPI + gap tables")
        assert kpi["status"] == "ok"


class TestEdgeCases:

    def test_empty_dicts(self):
        r = _derive_pipeline_status({}, {})
        assert r["alerts_fired"] == 0
        assert r["total_rows"]   == 0

    def test_none_values_as_zero(self):
        pq = {"dq_pass_rows": None, "dq_failed_rows": None,
              "pii_inconsistent_rows": None}
        ss = {"outlier_tables": None}
        r = _derive_pipeline_status(pq, ss)
        assert r["alerts_fired"] == 0

    def test_only_dq_alert(self):
        pq = {"dq_pass_rows": 5000, "dq_failed_rows": 500,
              "pii_inconsistent_rows": 0}
        ss = {"outlier_tables": 0}
        assert _derive_pipeline_status(pq, ss)["alerts_fired"] == 1

    def test_only_structural_alert(self):
        pq = {"dq_pass_rows": 5000, "dq_failed_rows": 0,
              "pii_inconsistent_rows": 0}
        ss = {"outlier_tables": 2}
        assert _derive_pipeline_status(pq, ss)["alerts_fired"] == 1
