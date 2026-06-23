"""
tests/unit/test_null_handling.py

Unit tests for Silver VAL-02 null / whitespace normalisation from silver_layer.py.
The Silver layer converts empty strings to NULL via:
    F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c)))

This file tests the pure-Python replica used for unit testing without PySpark.

Author: Gresa Hasani — MLOps / AI Ops Engineer
"""

import pytest


def _to_null(value):
    """
    Pure-Python replica of VAL-02 in silver_layer.py.
    Trims the value; returns None if empty string, else returns stripped value.
    Does NOT convert "null"/"nan" — that is handled by the Bronze _ingest helper.
    """
    if value is None:
        return None
    stripped = str(value).strip()
    return None if stripped == "" else stripped


def normalise_row(row: dict) -> dict:
    """Apply _to_null to all string values in a metadata row."""
    return {k: _to_null(v) if isinstance(v, str) else v for k, v in row.items()}


# ── _to_null() tests

class TestToNull:

    def test_none_returns_none(self):
        assert _to_null(None) is None

    def test_empty_string_returns_none(self):
        assert _to_null("") is None

    def test_whitespace_only_returns_none(self):
        assert _to_null("   ") is None

    def test_tab_returns_none(self):
        assert _to_null("\t") is None

    def test_newline_returns_none(self):
        assert _to_null("\n") is None

    def test_mixed_whitespace_returns_none(self):
        assert _to_null("  \t\n  ") is None

    def test_valid_string_returned_stripped(self):
        assert _to_null("  customer_id  ") == "customer_id"

    def test_valid_string_returned_as_is(self):
        assert _to_null("Finance") == "Finance"

    def test_email_returned(self):
        assert _to_null("erza.ademii24@gmail.com") == "erza.ademii24@gmail.com"

    def test_security_classification_returned(self):
        assert _to_null("Confidential") == "Confidential"

    def test_zero_string_returned(self):
        # "0" is not empty — must not become None
        assert _to_null("0") == "0"

    def test_false_string_returned(self):
        # "false" is a valid boolean string, not empty
        assert _to_null("false") == "false"

    def test_numeric_string_returned(self):
        assert _to_null("42") == "42"

    def test_conf_abbreviation_returned(self):
        # "Conf" is normalised to "Confidential" in VAL-03, not VAL-02
        assert _to_null("Conf") == "Conf"

    def test_single_space_returns_none(self):
        assert _to_null(" ") is None


# ── VAL-02 row-level normalisation tests

class TestNormaliseRow:

    def test_all_empty_strings_become_none(self):
        row = {
            "column_desc": "",
            "data_steward": "  ",
            "term_name": "\t",
        }
        result = normalise_row(row)
        assert all(v is None for v in result.values())

    def test_mixed_row(self):
        row = {
            "column_name":             "order_id",
            "column_desc":             "",
            "security_classification": "Confidential",
            "data_steward":            "  ",
            "tag_value":               "Finance",
        }
        result = normalise_row(row)
        assert result["column_name"] == "order_id"
        assert result["column_desc"] is None
        assert result["security_classification"] == "Confidential"
        assert result["data_steward"] is None
        assert result["tag_value"] == "Finance"

    def test_non_string_values_pass_through(self):
        row = {
            "pii_flag":              True,
            "total_record_count":    10000,
            "invalid_record_count":  45,
        }
        result = normalise_row(row)
        assert result["pii_flag"] is True
        assert result["total_record_count"] == 10000

    def test_full_metadata_row(self):
        """Simulates a real column from the metadata CSV after Bronze ingestion."""
        row = {
            "column_id":                    "col_abc123",
            "column_name":                  "order_amount",
            "table_id":                     "tbl_xyz",
            "table_name":                   "orders",
            "database_name":                "finance_dw",
            "schema_name":                  "public",
            "system_name":                  "SAP",
            "column_desc":                  "",           # -> None
            "data_steward":                 "  ",         # -> None
            "security_classification":      "Confidential",
            "certification_level":          "Registered",
            "pii_flag":                     "false",
            "critical_data_element_flag":   "false",
            "term_name":                    "",           # -> None
            "term_description":             "\t",         # -> None
            "tag_value":                    "Finance",
            "total_record_count":           "10000",
            "invalid_record_count":         "45",
        }
        result = normalise_row(row)
        assert result["column_id"]              == "col_abc123"
        assert result["column_desc"]            is None
        assert result["data_steward"]           is None
        assert result["term_name"]              is None
        assert result["term_description"]       is None
        assert result["security_classification"]== "Confidential"
        assert result["tag_value"]              == "Finance"
        assert result["pii_flag"]               == "false"   # not a string to None
