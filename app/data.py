"""
app/data.py
All Gold-table reads using Databricks SQL connector.
Catalog: dbx_metadata_governance_dev.gold.*
"""

import os
import time
import logging
import threading
import requests
from contextlib import contextmanager
from databricks import sql

logger = logging.getLogger(__name__)

CATALOG   = os.environ.get("GOLD_CATALOG", "dbx_metadata_governance_dev")
SCHEMA    = os.environ.get("GOLD_SCHEMA",  "gold")
HTTP_PATH = f"/sql/1.0/warehouses/{os.environ.get('SQL_WAREHOUSE_ID', '')}"


def _ensure_https(host: str) -> str:
    host = host.strip().rstrip("/")
    if host and not host.startswith("https://"):
        host = f"https://{host}"
    return host


# ── Lazy cached OAuth token (avoids blocking import & reuses across queries)
_token_cache: dict = {"value": None, "expires": 0.0}
_token_lock = threading.Lock()


def get_sp_token() -> str:
    host          = _ensure_https(os.environ.get("DATABRICKS_HOST", ""))
    client_id     = os.environ.get("DATABRICKS_CLIENT_ID", "")
    client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET", "")
    fallback      = os.environ.get("DATABRICKS_TOKEN", "")

    if not client_id or not client_secret:
        logger.warning("DATABRICKS_CLIENT_ID/SECRET not set, falling back to DATABRICKS_TOKEN.")
        return fallback

    try:
        resp = requests.post(
            f"{host}/oidc/v1/token",
            data={"grant_type": "client_credentials", "scope": "all-apis"},
            auth=(client_id, client_secret),
            timeout=10,
        )
        resp.raise_for_status()
        token = resp.json().get("access_token", "")
        if not token:
            logger.error("Empty token returned from OIDC endpoint.")
            return fallback
        return token
    except Exception as e:
        logger.error(f"get_sp_token failed: {e} — falling back to DATABRICKS_TOKEN.")
        return fallback


def _get_token() -> str:
    """Return a cached token, refreshing only when it's about to expire."""
    with _token_lock:
        if _token_cache["value"] and time.time() < _token_cache["expires"]:
            return _token_cache["value"]
        token = get_sp_token()
        _token_cache["value"]   = token
        _token_cache["expires"] = time.time() + 3000  # 50-min TTL
        return token


def _get_host() -> str:
    return _ensure_https(os.environ.get("DATABRICKS_HOST", "")).replace("https://", "")


# ── Single shared connection (context manager)
@contextmanager
def _connect():
    """Open one SQL connection; all queries in a refresh share it."""
    with sql.connect(
        server_hostname=_get_host(),
        http_path=HTTP_PATH,
        access_token=_get_token(),
    ) as conn:
        yield conn


# ── SQL runner — accepts an optional open connection to avoid re-connecting
def _run(sql_str: str, conn=None) -> list[dict]:
    def _exec(c):
        with c.cursor() as cur:
            cur.execute(sql_str)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    if conn is not None:
        return _exec(conn)
    with _connect() as c:
        return _exec(c)


def g(table: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{table}"


# ── Data loaders — all accept an optional shared connection
def load_kpi_summary(conn=None) -> dict:
    rows = _run(f"""
        SELECT
            total_databases, total_schemas, total_tables, total_columns,
            certified_columns, documented_columns, registered_columns, unclassified_columns,
            pii_columns, critical_columns,
            ROUND(avg_completeness_pct, 1)      AS avg_completeness_pct,
            ROUND(100 - avg_dq_invalid_pct, 1)  AS dq_pass_pct,
            ROUND(avg_dq_invalid_pct, 2)         AS avg_dq_invalid_pct
        FROM {g('kpi_summary')} LIMIT 1
    """, conn)
    if not rows:
        return {}
    return {k: (float(v) if v is not None else 0.0) for k, v in rows[0].items()}


def load_table_governance(limit: int = 200, conn=None) -> list[dict]:
    return _run(f"""
        SELECT
            system_name, database_name, schema_name, table_name, table_desc,
            data_steward, column_count,
            ROUND(table_governance_score, 1)  AS governance_score,
            ROUND(avg_documentation_score, 1) AS documentation_score,
            ROUND(avg_quality_score, 1)        AS quality_score,
            pii_column_count, critical_column_count
        FROM {g('table_governance')}
        ORDER BY table_governance_score DESC
        LIMIT {limit}
    """, conn)


def load_gaps(limit: int = 300, conn=None) -> list[dict]:
    return _run(f"""
        SELECT
            database_name, schema_name, table_name, column_name,
            data_steward, security_classification, achieved_cert_level,
            ROUND(completeness_pct, 1) AS completeness_pct,
            ROUND(dq_invalid_pct, 2)   AS dq_invalid_pct,
            gap_reason
        FROM {g('governance_gaps')}
        ORDER BY completeness_pct ASC
        LIMIT {limit}
    """, conn)


def load_pipeline_quality(conn=None) -> dict:
    rows = _run(f"""
        SELECT dq_pass_rows, dq_failed_rows, pii_consistent_rows, pii_inconsistent_rows
        FROM {g('dlt_summary')} LIMIT 1
    """, conn)
    if not rows:
        return {}
    r     = rows[0]
    total = int(r.get("dq_pass_rows") or 0) + int(r.get("dq_failed_rows") or 0)
    r["dq_invalid_ratio_pct"] = (
        round(int(r.get("dq_failed_rows") or 0) * 100.0 / total, 2) if total > 0 else 0.0
    )
    return r


def load_structural_summary(conn=None) -> dict:
    rows = _run(f"""
        SELECT total_tables, standard_column_count, avg_columns_per_table,
               compliant_tables, outlier_tables, structural_consistency_pct
        FROM {g('structural_summary')} LIMIT 1
    """, conn)
    return rows[0] if rows else {}


def load_domain_summary(conn=None) -> list[dict]:
    return _run(f"""
        SELECT domain, column_count, table_count,
               ROUND(avg_governance_score, 1) AS avg_governance_score,
               certified_columns, documented_columns, certified_pct
        FROM {g('domain_summary')}
        WHERE domain != 'Unassigned' AND domain != 'nan'
        ORDER BY avg_governance_score DESC
    """, conn)


def load_pipeline_status(pq: dict = None, ss: dict = None, conn=None) -> dict:
    """Build pipeline status from pre-fetched pq/ss, or fetch them if not supplied."""
    if pq is None:
        pq = load_pipeline_quality(conn)
    if ss is None:
        ss = load_structural_summary(conn)

    dq_fail  = int(pq.get("dq_failed_rows") or 0)
    dq_pass  = int(pq.get("dq_pass_rows") or 0)
    pii_bad  = int(pq.get("pii_inconsistent_rows") or 0)
    outliers = int(ss.get("outlier_tables") or 0)
    total    = dq_pass + dq_fail

    steps = [
        {"name": "Bronze ingestion",       "status": "ok",
         "detail": f"{total:,} rows ingested from Azure Blob"},
        {"name": "Silver validation",      "status": "warn" if dq_fail > 0 else "ok",
         "detail": f"{dq_fail} rows exceed DQ threshold · routed to quarantine" if dq_fail > 0
                   else f"All {total:,} rows pass DQ threshold"},
        {"name": "Gold scoring",           "status": "ok",
         "detail": "11 rubric checks applied · Gold tables written"},
        {"name": "KPI + gap tables",       "status": "ok",
         "detail": "kpi_summary · governance_gaps · dlt_summary refreshed"},
        {"name": "Structural consistency", "status": "warn" if outliers > 0 else "ok",
         "detail": f"{outliers} structural outlier(s) detected" if outliers > 0
                   else "All tables match structural standard"},
        {"name": "Sensitivity check",      "status": "warn" if pii_bad > 0 else "ok",
         "detail": f"{pii_bad} PII columns with inconsistent flags" if pii_bad > 0
                   else "All PII flags consistent"},
    ]

    return {
        "steps":           steps,
        "quarantine_rows": dq_fail,
        "total_rows":      total,
        "alerts_fired":    sum([dq_fail > 0, pii_bad > 0, outliers > 0]),
    }


def load_alerts(conn=None) -> list[dict]:
    try:
        return _run(f"""
            SELECT
                'Data Quality Below 95%'        AS name,
                CASE WHEN ROUND(100 - avg_dq_invalid_pct, 1) < 95
                     THEN 'triggered' ELSE 'ok' END AS status,
                'erza.ademii24@gmail.com'        AS owner
            FROM {g('kpi_summary')}
            UNION ALL
            SELECT
                'Metadata Completeness Below 90%',
                CASE WHEN avg_completeness_pct < 90
                     THEN 'triggered' ELSE 'ok' END,
                'erza.ademii24@gmail.com'
            FROM {g('kpi_summary')}
            UNION ALL
            SELECT
                'Structural Consistency Below 85%',
                CASE WHEN structural_consistency_pct < 85
                     THEN 'triggered' ELSE 'ok' END,
                'erza.ademii24@gmail.com'
            FROM {g('structural_summary')}
        """, conn)
    except Exception as e:
        logger.warning(f"Could not load alerts: {e}")
        return []
