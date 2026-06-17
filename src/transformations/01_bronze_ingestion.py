# Purpose: Ingestion of Data
# Author: Bledi Rexha
# Last updated: 2026-06-16
# Dependencies: Delta Live Tables, pandas, PySpark, Azure Blob SAS URI
# Change log:
#   - FIX (null integrity): pd.read_csv(dtype=str) returns NaN for blank cells,
#     and spark.createDataFrame() converts NaN on StringType columns into the
#     LITERAL STRING "nan". That silently passed every not_null governance check
#     (a missing column_desc arrived as "nan", which is non-null), so no asset
#     could ever score None or Registered, and blank database/table names
#     fabricated phantom "nan" catalog entries. We now coerce NaN and common
#     textual null sentinels to real Python None -> SQL NULL before createDataFrame.
import dlt
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp, lit

SAS_URI = "https://capstonemetadata.blob.core.windows.net/sources/metadata.csv?sp=r&st=2026-06-14T10:48:51Z&se=2026-07-03T19:03:51Z&spr=https&sv=2026-02-06&sr=b&sig=I%2F2Ki3d%2FjW5iTDCy%2BhJ2IQFk2Bg4wpz2ophitWKlTLk%3D"


# 26 columns in source order.
metadata_schema = StructType([
    StructField("column_id", StringType(), True),
    StructField("column_name", StringType(), True),
    StructField("column_desc", StringType(), True),
    StructField("term_name", StringType(), True),
    StructField("term_description", StringType(), True),
    StructField("security_classification", StringType(), True),
    StructField("critical_data_element_flag", StringType(), True),
    StructField("pii_flag", StringType(), True),
    StructField("term_subdomain", StringType(), True),
    StructField("data_steward", StringType(), True),
    StructField("table_id", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("table_desc", StringType(), True),
    StructField("table_owner_in_source", StringType(), True),
    StructField("schema_id", StringType(), True),
    StructField("schema_name", StringType(), True),
    StructField("database_id", StringType(), True),
    StructField("database_name", StringType(), True),
    StructField("system_id", StringType(), True),
    StructField("system_name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("total_record_count", StringType(), True),
    StructField("invalid_record_count", StringType(), True),
    StructField("tag_name", StringType(), True),
    StructField("tag_value", StringType(), True),
    StructField("certification_level", StringType(), True),
])

# Values that must be treated as missing (real NULL), not as text.
_NULL_TOKENS = {"", "nan", "NaN", "NAN", "null", "NULL", "None", "none", "NA", "N/A"}


def _to_null(v):
    # NaN (float), None, or any textual null-sentinel -> real None; else trimmed string.
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    return None if s in _NULL_TOKENS else s


@dlt.table(
    name="metadata_raw_bronze",
    comment="Raw metadata ingested from Azure Blob Storage into the Bronze layer"
)
def metadata_raw_bronze():
    pdf = pd.read_csv(SAS_URI, dtype=str)
    expected_columns = [field.name for field in metadata_schema.fields]
    pdf = pdf[expected_columns]

    # CRITICAL: convert NaN / textual null-sentinels to real None BEFORE
    # createDataFrame, so blanks land as SQL NULL instead of the string "nan".
    for c in expected_columns:
        pdf[c] = pdf[c].map(_to_null)

    df = spark.createDataFrame(pdf, schema=metadata_schema)
    return (
        df
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", lit(SAS_URI.split("?")[0]))
    )