# Purpose: Silver layer - type casting, normalization, deterministic surrogate
#          keys, logical-column dedup, rubric-driven GATED certification.
# Author: Erza Ademi - Data Quality & Rules Engineer (R5)
# Last updated: 2026-06-14
# Dependencies:  - dbx_metadata_governance_dev.bronze.metadata_raw_bronze
#                - /Volumes/.../silver/rubric/rubric.yaml (rubric_version 2.0)
# Change log:
#   - VAL-01 try_cast (ANSI-safe): counts are float-formatted strings with
#     blanks; direct string->bigint throws CAST_INVALID_INPUT under ANSI mode.
#   - VAL-03b rebuilds surrogate keys from the NAME hierarchy ROOTED AT
#     system_name (root-cause fix for per-row-unique source ids). The same
#     column name in different source systems (e.g. crm.silver.opportunities
#     .probability in Snowflake vs Salesforce) is a DISTINCT asset, so
#     system_name is part of the key. Originals preserved as source_*_id.
#   - VAL-04 dedups at the real asset grain (one row per
#     system.db.schema.table.column), keeping the most COMPLETE snapshot via a
#     deterministic ordering (not arbitrary latest-by-timestamp).
#   - VAL-08 is now GATED (rubric v2.0): a level is earned only by passing ALL of
#     that level's gate_checks (cumulative), not by a count of passed checks.

import dlt
import yaml
from pyspark.sql import functions as F
from pyspark.sql.window import Window

DEFAULT_RUBRIC_PATH = (
    "/Volumes/dbx_metadata_governance_dev/silver/rubric/rubric.yaml"
)

def _rubric_path():
    try:
        return spark.conf.get("rubric.path")
    except Exception:
        return DEFAULT_RUBRIC_PATH

def load_rubric(path):
    with open(path, "r") as f:
        rubric = yaml.safe_load(f)
    if "checks" not in rubric or not rubric["checks"]:
        raise ValueError(f"No 'checks' section found in rubric at {path}")
    if "certification_levels" not in rubric or not rubric["certification_levels"]:
        raise ValueError(f"No 'certification_levels' section found in rubric at {path}")
    return rubric

def _check_expression(check):
    rule = check.get("rule")

    if rule == "not_null":
        return F.col(check["field"]).isNotNull()

    if rule == "not_null_all":
        expr = None
        for fld in check["fields"]:
            cond = F.col(fld).isNotNull()
            expr = cond if expr is None else (expr & cond)
        if check.get("condition"):
            expr = expr & F.expr(check["condition"])
        return expr

    if rule == "allowed_values":
        return F.col(check["field"]).isin(check["allowed"])

    if rule == "ratio_below_threshold":
        numerator, denominator = check["fields"][0], check["fields"][1]
        threshold = float(check["threshold"])
        return F.when(
            F.col(denominator) > 0,
            (F.col(numerator) / F.col(denominator)) < F.lit(threshold),
        ).otherwise(F.lit(None).cast("boolean"))

    raise ValueError(
        f"Unsupported rule '{rule}' in check {check.get('id', '<no id>')}"
    )

def _referenced_fields(check):
    fields = []
    if "field" in check:
        fields.append(check["field"])
    if "fields" in check:
        fields.extend(check["fields"])
    return fields

def _check_column_name(check):
    return "check_" + check["name"]

# ----------------------------------------------------------------------------
# Surrogate-key helpers. A deterministic id is built from a lowercased, trimmed
# name path. F.concat is null-propagating: if any name component is NULL the key
# is NULL -> NULL id -> the row is dropped by the expectations (orphaned asset
# with no catalog parent). The same real asset always gets the same id across
# runs and region copies, which is what makes COUNT(DISTINCT) trustworthy.
# ----------------------------------------------------------------------------

def _norm(col_name):
    return F.lower(F.trim(F.col(col_name)))

def _key(*name_cols):
    parts = []
    for i, c in enumerate(name_cols):
        if i > 0:
            parts.append(F.lit("||"))
        parts.append(_norm(c))
    return F.concat(*parts)

def _sid(key_col):
    return F.when(key_col.isNotNull(), F.sha2(key_col, 256))


@dlt.table(
    name="metadata_validated",
    comment="Silver: typed, normalized, key-rebuilt, deduplicated metadata; gated certification driven by rubric.yaml",
)
@dlt.expect_or_drop("valid_column_id", "column_id IS NOT NULL")
@dlt.expect_or_drop("valid_column_name", "column_name IS NOT NULL")
@dlt.expect_or_drop("valid_table_id", "table_id IS NOT NULL")
def metadata_validated():

    df = spark.read.table("dbx_metadata_governance_dev.bronze.metadata_raw_bronze")

    # VAL-01: Type casting (ANSI-safe).
    df = df \
        .withColumn("pii_flag", F.expr("try_cast(pii_flag AS BOOLEAN)")) \
        .withColumn("critical_data_element_flag", F.expr("try_cast(critical_data_element_flag AS BOOLEAN)")) \
        .withColumn("total_record_count", F.expr("CAST(try_cast(total_record_count AS DOUBLE) AS BIGINT)")) \
        .withColumn("invalid_record_count", F.expr("CAST(try_cast(invalid_record_count AS DOUBLE) AS BIGINT)"))

    # VAL-02: Normalize strings (trim; empty string -> NULL)
    string_cols = [c for c, t in df.dtypes if t == "string"]
    for c in string_cols:
        df = df.withColumn(
            c,
            F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c)))
        )

    # VAL-03: Standardize values
    df = df \
        .withColumn(
            "security_classification",
            F.when(F.col("security_classification") == "Conf", "Confidential")
             .otherwise(F.col("security_classification"))
        ) \
        .withColumn(
            "certification_level",
            F.initcap(F.col("certification_level"))
        )

    # VAL-03b: REBUILD surrogate keys from the name hierarchy (ROOT-CAUSE FIX).
    # Preserve the original random ids for lineage, then overwrite with
    # deterministic, name-derived hashes.
    df = df \
        .withColumnRenamed("database_id", "source_database_id") \
        .withColumnRenamed("schema_id", "source_schema_id") \
        .withColumnRenamed("table_id", "source_table_id") \
        .withColumnRenamed("column_id", "source_column_id")

    df = df \
        .withColumn("database_id", _sid(_key("system_name", "database_name"))) \
        .withColumn("schema_id",   _sid(_key("system_name", "database_name", "schema_name"))) \
        .withColumn("table_id",    _sid(_key("system_name", "database_name", "schema_name", "table_name"))) \
        .withColumn("column_id",   _sid(_key("system_name", "database_name", "schema_name", "table_name", "column_name")))

    # VAL-04: Dedup on the rebuilt logical column_id. After the key includes
    # system_name the remaining repeats are time-snapshots of the SAME asset
    # (total/invalid_record_count differ between profiling runs), NOT distinct
    # columns. We keep the most COMPLETE snapshot rather than an arbitrary
    # "latest": for a single batch load the ingestion timestamps are
    # near-identical, so ordering on them is non-deterministic and breaks the
    # idempotency test. _completeness counts populated governance attributes;
    # source_column_id (the preserved per-row-unique source id) is a stable
    # tiebreak so the surviving row is reproducible across runs.
    _gov_fields = [
        "column_desc", "term_name", "data_steward", "security_classification",
        "pii_flag", "critical_data_element_flag", "tag_value",
    ]
    completeness_expr = F.lit(0)
    for _c in _gov_fields:
        completeness_expr = completeness_expr + F.col(_c).isNotNull().cast("int")
    df = df.withColumn("_completeness", completeness_expr)

    w = Window.partitionBy("column_id").orderBy(
        F.col("_completeness").desc(),
        F.col("source_column_id").asc(),
    )
    df = df.withColumn("_rn", F.row_number().over(w)) \
           .filter(F.col("_rn") == 1) \
           .drop("_rn", "_completeness")

    # VAL-05: Load rubric and validate that every referenced field exists.
    rubric = load_rubric(_rubric_path())
    checks = rubric["checks"]

    required_fields = sorted({
        fld for chk in checks for fld in _referenced_fields(chk)
    })
    missing = [fld for fld in required_fields if fld not in df.columns]
    if missing:
        raise ValueError(
            "rubric.yaml references columns not present in metadata_raw_bronze: "
            + ", ".join(missing)
            + ". Add them upstream or update the rubric."
        )

    # VAL-06: Build one boolean column per rubric check.
    check_names = []
    id_to_check_col = {}
    for chk in checks:
        col_name = _check_column_name(chk)
        df = df.withColumn(col_name, _check_expression(chk))
        check_names.append(col_name)
        id_to_check_col[chk["id"]] = col_name

    total_checks = len(check_names)  # expected 11

    # VAL-07: Informational scores (NOT used for certification). NULL = not passed.
    passed_expr = F.lit(0)
    for col_name in check_names:
        passed_expr = passed_expr + F.coalesce(F.col(col_name).cast("int"), F.lit(0))

    df = df \
        .withColumn("checks_passed", passed_expr) \
        .withColumn("checks_total", F.lit(total_checks)) \
        .withColumn(
            "completeness_pct",
            F.col("checks_passed") / F.lit(total_checks) * 100
        ) \
        .withColumn(
            "dq_invalid_pct",
            F.when(
                F.col("total_record_count") > 0,
                F.col("invalid_record_count") / F.col("total_record_count") * 100
            ).otherwise(None)
        )

    # VAL-08: GATED certification. A level is earned only if ALL of its
    # gate_checks pass (cumulative). Highest satisfied level wins; else 'None'.
    # NULL check results count as not-passed (coalesce to False).
    def _gate_expr(level):
        gate = None
        for cid in level["gate_checks"]:
            if cid not in id_to_check_col:
                raise ValueError(
                    f"certification level '{level['name']}' references unknown check id '{cid}'"
                )
            passed = F.coalesce(F.col(id_to_check_col[cid]).cast("boolean"), F.lit(False))
            gate = passed if gate is None else (gate & passed)
        return gate

    levels = sorted(
        rubric["certification_levels"],
        key=lambda lvl: len(lvl["gate_checks"]),
        reverse=True,
    )
    cert_expr = None
    for lvl in levels:
        gate = _gate_expr(lvl)
        cert_expr = F.when(gate, F.lit(lvl["name"])) if cert_expr is None \
            else cert_expr.when(gate, F.lit(lvl["name"]))
    cert_expr = cert_expr.otherwise(F.lit("None"))

    df = df.withColumn("achieved_cert_level", cert_expr)

    return df