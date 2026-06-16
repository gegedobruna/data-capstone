"""
app/alerts.py
Email alerts handled by Databricks directly.
This engine only evaluates rules and returns fired alerts for the UI.
"""

import os
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

ALERT_RULES = [
    {
        "id":       "dq_ratio_exceeded",
        "name":     "DQ invalid ratio exceeded",
        "severity": "Critical",
        "check": lambda kpis, pq, ss, gaps: (
            (int(pq.get("dq_failed_rows") or 0) * 100.0
             / max(int(pq.get("dq_pass_rows") or 0) + int(pq.get("dq_failed_rows") or 0), 1))
            > float(os.environ.get("ALERT_DQ_THRESHOLD", "5"))
        ),
        "message": lambda kpis, pq, ss, gaps: (
            f"DQ invalid ratio exceeded {os.environ.get('ALERT_DQ_THRESHOLD','5')}% threshold. "
            f"Failed rows: {int(pq.get('dq_failed_rows', 0))}."
        ),
    },
    {
        "id":       "quarantine_spike",
        "name":     "Quarantine spike",
        "severity": "High",
        "check": lambda kpis, pq, ss, gaps: (
            int(pq.get("dq_failed_rows") or 0)
            > int(os.environ.get("ALERT_QUARANTINE_MAX", "50"))
        ),
        "message": lambda kpis, pq, ss, gaps: (
            f"{int(pq.get('dq_failed_rows', 0))} rows quarantined — "
            f"exceeds limit of {os.environ.get('ALERT_QUARANTINE_MAX','50')}."
        ),
    },
    {
        "id":       "pii_inconsistent",
        "name":     "PII flags inconsistent",
        "severity": "High",
        "check": lambda kpis, pq, ss, gaps: int(pq.get("pii_inconsistent_rows") or 0) > 0,
        "message": lambda kpis, pq, ss, gaps: (
            f"{int(pq.get('pii_inconsistent_rows', 0))} PII columns with inconsistent flags (CHK-08)."
        ),
    },
    {
        "id":       "structural_outliers",
        "name":     "Structural outliers detected",
        "severity": "High",
        "check": lambda kpis, pq, ss, gaps: int(ss.get("outlier_tables") or 0) > 0,
        "message": lambda kpis, pq, ss, gaps: (
            f"{int(ss.get('outlier_tables', 0))} table(s) deviate from structural standard."
        ),
    },
    {
        "id":       "pii_without_classification",
        "name":     "PII without classification",
        "severity": "High",
        "check": lambda kpis, pq, ss, gaps: any(
            "security" in (g.get("gap_reason") or "").lower()
            for g in (gaps or [])
        ),
        "message": lambda kpis, pq, ss, gaps: (
            "PII-flagged columns have no security_classification (CHK-07 fail)."
        ),
    },
]

_FIRED: set = set()


class AlertEngine:
    def evaluate_and_fire(self, kpis, pq, ss, gaps) -> list:
        fired_now = []
        for rule in ALERT_RULES:
            rule_id = rule["id"]
            try:
                triggered = rule["check"](kpis, pq, ss, gaps)
            except Exception as e:
                logger.warning(f"Alert rule {rule_id} error: {e}")
                triggered = False

            if triggered and rule_id not in _FIRED:
                _FIRED.add(rule_id)
                msg = rule["message"](kpis, pq, ss, gaps)
                fired_now.append({
                    "rule_id":  rule_id,
                    "name":     rule["name"],
                    "severity": rule["severity"],
                    "message":  msg,
                    "fired_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
                })
                logger.info(f"Alert fired: {rule_id} — {msg}")
        return fired_now

    def reset(self):
        _FIRED.clear()
