"""
tests/integration/test_silver_transform.py

Integration tests for the Silver layer transformation logic from silver_layer.py.
Tests VAL-01 through VAL-08 end-to-end using PySpark local mode.

Covers:
  - VAL-01: Type casting (try_cast ANSI-safe)
  - VAL-02: String normalisation (empty -> NULL)
  - VAL-03: Value standardisation (Conf -> Confidential, initcap cert level)
  - VAL-03b: Deterministic surrogate key rebuild from name hierarchy
  - VAL-04: Deduplication — keeps most complete row per column_id
  - VAL-06: 11 boolean DQ check columns computed
  - VAL-07: completeness_pct and dq_invalid_pct
  - VAL-08: Gated certification level (cumulative gate_checks)

Requires: pyspark (pip install pyspark)
Run with: pytest tests/integration/test_silver_transform.py -v

Author: Erza Ademi — Data Quality & Rules Engineer
        Gresa Hasani — MLOps / AI Ops Engineer
"""

import pytest

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, StringType
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not PYSPARK_AVAILABLE,
    reason="PySpark not installed — run: pip install pyspark"
)


# ── Pure-Python replicas of silver_layer.py helpers for Spark-free unit paths

SAMPLE_RUBRIC = {
    "rubric_version": "2.0",
    "checks": [
        {"id": "CHK-01", "name": "column_description_present",    "rule": "not_null",       "field": "column_desc"},
        {"id": "CHK-02", "name": "business_term_linked",          "rule": "not_null",       "field": "term_name"},
        {"id": "CHK-03", "name": "security_classification_present","rule": "allowed_values", "field": "security_classification",
         "allowed": ["Public", "Internal", "Confidential", "Restricted"]},
        {"id": "CHK-04", "name": "data_steward_assigned",         "rule": "not_null",       "field": "data_steward"},
        {"id": "CHK-05", "name": "domain_tag_present",            "rule": "not_null",       "field": "tag_value"},
        {"id": "CHK-06", "name": "sensitivity_flags_set",         "rule": "not_null_all",   "fields": ["pii_flag", "critical_data_element_flag"]},
        {"id": "CHK-07", "name": "certification_level_populated", "rule": "allowed_values", "field": "certification_level",
         "allowed": ["Registered", "Documented", "Certified"]},
        {"id": "CHK-08", "name": "schema_assignment_valid",       "rule": "not_null_all",   "fields": ["schema_name", "database_name"]},
        {"id": "CHK-09", "name": "invalid_record_ratio_within_threshold", "rule": "ratio_below_threshold",
         "fields": ["invalid_record_count", "total_record_count"], "threshold": "0.10"},
        {"id": "CHK-10", "name": "pii_consistency",               "rule": "not_null",       "field": "pii_flag"},
        {"id": "CHK-11", "name": "structural_standard_met",       "rule": "not_null",       "field": "table_name"},
    ],
    "certification_levels": [
        {"name": "Registered",  "gate_checks": ["CHK-03", "CHK-08"]},
        {"name": "Documented",  "gate_checks": ["CHK-03", "CHK-08", "CHK-01", "CHK-02", "CHK-04", "CHK-05"]},
        {"name": "Certified",   "gate_checks": ["CHK-03", "CHK-08", "CHK-01", "CHK-02", "CHK-04", "CHK-05",
                                                 "CHK-07", "CHK-06", "CHK-09", "CHK-10"]},
    ]
}


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("silver_layer_tests")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


def _base_schema():
    cols = [
        "source_column_id", "column_name", "table_name", "database_name",
        "schema_name", "system_name", "column_desc", "data_steward",
        "security_classification", "certification_level", "pii_flag",
        "critical_data_element_flag", "term_name", "term_description",
        "term_subdomain", "tag_value", "total_record_count", "invalid_record_count",
    ]
    return StructType([StructField(c, StringType(), True) for c in cols])


def _good_row(**overrides):
    base = {
        "source_column_id":          "src_001",
        "column_name":               "customer_id",
        "table_name":                "customers",
        "database_name":             "crm",
        "schema_name":               "public",
        "system_name":               "Salesforce",
        "column_desc":               "Unique customer identifier",
        "data_steward":              "erza.ademii24@gmail.com",
        "security_classification":   "Confidential",
        "certification_level":       "Certified",
        "pii_flag":                  "true",
        "critical_data_element_flag":"true",
        "term_name":                 "Customer Identifier",
        "term_description":          "Primary key for customer entity",
        "term_subdomain":            "Customer",
        "tag_value":                 "CRM",
        "total_record_count":        "50000",
        "invalid_record_count":      "200",
    }
    base.update(overrides)
    return base


def _apply_silver_transforms(spark, rows):
    """
    Pure-Python implementation of the Silver layer transforms for testing.
    Mirrors silver_layer.py logic without DLT decorators.
    """
    from pyspark.sql.window import Window
    import hashlib

    schema = _base_schema()
    df = spark.createDataFrame([list(r.values()) for r in rows], schema=schema)

    # VAL-01: Type cast
    df = (df
        .withColumn("pii_flag",                  F.expr("try_cast(pii_flag AS BOOLEAN)"))
        .withColumn("critical_data_element_flag", F.expr("try_cast(critical_data_element_flag AS BOOLEAN)"))
        .withColumn("total_record_count",         F.expr("CAST(try_cast(total_record_count AS DOUBLE) AS BIGINT)"))
        .withColumn("invalid_record_count",       F.expr("CAST(try_cast(invalid_record_count AS DOUBLE) AS BIGINT)"))
    )

    # VAL-02: String normalisation
    string_cols = [f.name for f in schema.fields]
    for c in string_cols:
        if c in df.columns and dict(df.dtypes).get(c) == "string":
            df = df.withColumn(c, F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c))))

    # VAL-03: Value standardisation
    df = (df
        .withColumn("security_classification",
                    F.when(F.col("security_classification") == "Conf", "Confidential")
                     .otherwise(F.col("security_classification")))
        .withColumn("certification_level", F.initcap(F.col("certification_level")))
    )

    # VAL-03b: Surrogate key rebuild
    def _key(*cols):
        parts = []
        for i, c in enumerate(cols):
            if i > 0: parts.append(F.lit("||"))
            parts.append(F.lower(F.trim(F.col(c))))
        return F.concat(*parts)

    def _sid(key_col):
        return F.when(key_col.isNotNull(), F.sha2(key_col, 256))

    df = (df
        .withColumn("database_id", _sid(_key("system_name", "database_name")))
        .withColumn("schema_id",   _sid(_key("system_name", "database_name", "schema_name")))
        .withColumn("table_id",    _sid(_key("system_name", "database_name", "schema_name", "table_name")))
        .withColumn("column_id",   _sid(_key("system_name", "database_name", "schema_name", "table_name", "column_name")))
    )

    # VAL-04: Dedup — keep most complete row per column_id
    gov_fields = ["column_desc", "term_name", "data_steward", "security_classification",
                  "pii_flag", "critical_data_element_flag", "tag_value"]
    completeness_expr = sum(
        F.col(c).isNotNull().cast("int") for c in gov_fields
    )
    df = df.withColumn("_completeness", completeness_expr)
    w = Window.partitionBy("column_id").orderBy(
        F.col("_completeness").desc(), F.col("source_column_id").asc()
    )
    df = (df.withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn", "_completeness"))

    # VAL-06: 11 check columns
    checks_meta = SAMPLE_RUBRIC["checks"]
    for chk in checks_meta:
        col_name = "check_" + chk["name"]
        rule = chk["rule"]
        if rule == "not_null":
            df = df.withColumn(col_name, F.col(chk["field"]).isNotNull())
        elif rule == "not_null_all":
            expr = None
            for fld in chk["fields"]:
                cond = F.col(fld).isNotNull()
                expr = cond if expr is None else (expr & cond)
            df = df.withColumn(col_name, expr)
        elif rule == "allowed_values":
            df = df.withColumn(col_name, F.col(chk["field"]).isin(chk["allowed"]))
        elif rule == "ratio_below_threshold":
            n, d = chk["fields"][0], chk["fields"][1]
            t = float(chk["threshold"])
            df = df.withColumn(col_name,
                F.when(F.col(d) > 0, (F.col(n) / F.col(d)) < F.lit(t)).otherwise(None))

    # VAL-07: Scores
    check_names = ["check_" + c["name"] for c in checks_meta]
    passed_expr = sum(F.coalesce(F.col(cn).cast("int"), F.lit(0)) for cn in check_names)
    df = (df
        .withColumn("checks_passed",     passed_expr)
        .withColumn("checks_total",      F.lit(len(check_names)))
        .withColumn("completeness_pct",  F.col("checks_passed") / F.lit(len(check_names)) * 100)
        .withColumn("dq_invalid_pct",
                    F.when(F.col("total_record_count") > 0,
                           F.col("invalid_record_count") / F.col("total_record_count") * 100)
                     .otherwise(None))
    )

    # VAL-08: Gated cert level
    id_to_col = {"check_" + c["name"]: "check_" + c["name"] for c in checks_meta}
    id_map    = {c["id"]: "check_" + c["name"] for c in checks_meta}
    levels    = sorted(SAMPLE_RUBRIC["certification_levels"],
                       key=lambda l: len(l["gate_checks"]), reverse=True)

    cert_expr = None
    for lvl in levels:
        gate = None
        for cid in lvl["gate_checks"]:
            col_n = id_map[cid]
            passed = F.coalesce(F.col(col_n).cast("boolean"), F.lit(False))
            gate = passed if gate is None else (gate & passed)
        cert_expr = F.when(gate, F.lit(lvl["name"])) if cert_expr is None \
                    else cert_expr.when(gate, F.lit(lvl["name"]))
    cert_expr = cert_expr.otherwise(F.lit("None"))
    df = df.withColumn("achieved_cert_level", cert_expr)

    return df


# ── Tests

class TestVAL01TypeCast:

    def test_pii_flag_cast_to_boolean(self, spark):
        df = _apply_silver_transforms(spark, [_good_row(pii_flag="true")])
        assert df.schema["pii_flag"].dataType.simpleString() == "boolean"

    def test_total_record_count_cast_to_bigint(self, spark):
        df = _apply_silver_transforms(spark, [_good_row(total_record_count="10000")])
        dtype = df.schema["total_record_count"].dataType.simpleString()
        assert dtype == "bigint"

    def test_float_formatted_count_cast(self, spark):
        # "10000.0" is a common CSV artifact — must survive try_cast
        df = _apply_silver_transforms(spark, [_good_row(total_record_count="10000.0")])
        row = df.first()
        assert row["total_record_count"] == 10000

    def test_blank_count_becomes_null(self, spark):
        df = _apply_silver_transforms(spark, [_good_row(total_record_count="")])
        row = df.first()
        assert row["total_record_count"] is None


class TestVAL02StringNorm:

    def test_empty_string_becomes_null(self, spark):
        df = _apply_silver_transforms(spark, [_good_row(column_desc="")])
        assert df.first()["column_desc"] is None

    def test_whitespace_becomes_null(self, spark):
        df = _apply_silver_transforms(spark, [_good_row(data_steward="   ")])
        assert df.first()["data_steward"] is None

    def test_valid_string_trimmed(self, spark):
        df = _apply_silver_transforms(spark, [_good_row(column_desc="  Order amount  ")])
        assert df.first()["column_desc"] == "Order amount"


class TestVAL03Standardise:

    def test_conf_normalised_to_confidential(self, spark):
        df = _apply_silver_transforms(spark, [_good_row(security_classification="Conf")])
        assert df.first()["security_classification"] == "Confidential"

    def test_valid_classification_unchanged(self, spark):
        df = _apply_silver_transforms(spark, [_good_row(security_classification="Public")])
        assert df.first()["security_classification"] == "Public"

    def test_cert_level_initcapped(self, spark):
        df = _apply_silver_transforms(spark, [_good_row(certification_level="certified")])
        assert df.first()["certification_level"] == "Certified"


class TestVAL03bSurrogateKeys:

    def test_column_id_is_sha256(self, spark):
        df = _apply_silver_transforms(spark, [_good_row()])
        col_id = df.first()["column_id"]
        assert col_id is not None
        assert len(col_id) == 64  # SHA-256 hex

    def test_same_column_same_system_same_id(self, spark):
        row_a = _good_row(source_column_id="src_001")
        row_b = _good_row(source_column_id="src_002")  # different source id, same name path
        df = _apply_silver_transforms(spark, [row_a, row_b])
        ids = [r["column_id"] for r in df.collect()]
        # After dedup, only one row should survive
        assert len(ids) == 1

    def test_same_name_different_system_different_id(self, spark):
        row_a = _good_row(system_name="Salesforce", source_column_id="src_001")
        row_b = _good_row(system_name="SAP",        source_column_id="src_002")
        df = _apply_silver_transforms(spark, [row_a, row_b])
        ids = {r["column_id"] for r in df.collect()}
        assert len(ids) == 2  # different system -> different id


class TestVAL04Dedup:

    def test_duplicate_column_id_deduped_to_one(self, spark):
        row_a = _good_row(source_column_id="src_001", column_desc="Desc A")
        row_b = _good_row(source_column_id="src_002", column_desc="Desc B")
        df = _apply_silver_transforms(spark, [row_a, row_b])
        assert df.count() == 1

    def test_more_complete_row_kept(self, spark):
        # row_a is less complete (no tag_value)
        row_a = _good_row(source_column_id="src_001", tag_value="")
        # row_b is more complete (has tag_value)
        row_b = _good_row(source_column_id="src_002", tag_value="CRM")
        df = _apply_silver_transforms(spark, [row_a, row_b])
        surviving = df.first()
        assert surviving["tag_value"] == "CRM"


class TestVAL06CheckColumns:

    def test_11_check_columns_present(self, spark):
        df = _apply_silver_transforms(spark, [_good_row()])
        check_cols = [c for c in df.columns if c.startswith("check_")]
        assert len(check_cols) == 11

    def test_all_checks_true_for_good_row(self, spark):
        df = _apply_silver_transforms(spark, [_good_row()])
        row = df.first()
        check_cols = [c for c in df.columns if c.startswith("check_")]
        failed = [c for c in check_cols if not row[c]]
        assert failed == [], f"Unexpected failing checks: {failed}"

    def test_null_steward_fails_steward_check(self, spark):
        df = _apply_silver_transforms(spark, [_good_row(data_steward="")])
        assert df.first()["check_data_steward_assigned"] is False


class TestVAL07Scores:

    def test_completeness_pct_100_for_good_row(self, spark):
        df = _apply_silver_transforms(spark, [_good_row()])
        pct = df.first()["completeness_pct"]
        assert pct == pytest.approx(100.0, abs=1.0)

    def test_dq_invalid_pct_computed(self, spark):
        df = _apply_silver_transforms(spark, [_good_row(
            total_record_count="10000", invalid_record_count="500"
        )])
        pct = df.first()["dq_invalid_pct"]
        assert pct == pytest.approx(5.0, abs=0.1)

    def test_dq_invalid_pct_null_when_no_total(self, spark):
        df = _apply_silver_transforms(spark, [_good_row(total_record_count="")])
        assert df.first()["dq_invalid_pct"] is None


class TestVAL08GatedCertLevel:

    def test_good_row_achieves_certified(self, spark):
        df = _apply_silver_transforms(spark, [_good_row()])
        assert df.first()["achieved_cert_level"] == "Certified"

    def test_missing_steward_drops_to_registered(self, spark):
        df = _apply_silver_transforms(spark, [_good_row(
            data_steward="", tag_value="", term_name=""
        )])
        level = df.first()["achieved_cert_level"]
        assert level == "Registered"

    def test_invalid_classification_achieves_none(self, spark):
        df = _apply_silver_transforms(spark, [_good_row(security_classification="TopSecret")])
        assert df.first()["achieved_cert_level"] == "None"

    def test_high_dq_invalid_blocks_certified(self, spark):
        # 20% invalid — above 10% threshold
        df = _apply_silver_transforms(spark, [_good_row(
            total_record_count="10000", invalid_record_count="2000"
        )])
        level = df.first()["achieved_cert_level"]
        assert level in ("Documented", "Registered")

    def test_conf_abbreviation_after_normalise_achieves_certified(self, spark):
        # "Conf" is normalised to "Confidential" in VAL-03 before CHK-03 fires
        df = _apply_silver_transforms(spark, [_good_row(security_classification="Conf")])
        assert df.first()["achieved_cert_level"] == "Certified"
