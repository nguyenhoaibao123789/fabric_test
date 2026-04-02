"""
shared_functions.py — Shared helper library for Fabric notebooks.
  - azure-storage-file-datalake  (OneLake)
  - Spark JDBC                   (Fabric Data Warehouse)
  - mssparkutils.credentials     (Azure Key Vault)
"""
from __future__ import annotations

import datetime
import fnmatch
import logging
import random
import re
import time
from collections import namedtuple

import com.microsoft.spark.fabric  # noqa: F401
from azure.storage.filedatalake import DataLakeFileClient, DataLakeServiceClient
from com.microsoft.spark.fabric.Constants import Constants  # noqa: F401
from notebookutils import mssparkutils
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *  # noqa: F403

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Fabric token credential
# ---------------------------------------------------------------------------

AccessToken = namedtuple("AccessToken", ["token", "expires_on"])


class _FabricTokenCredential:
    """Wraps mssparkutils.credentials.getToken() to match the
    azure.identity credential interface (.get_token())."""

    def get_token(self, *scopes, **kwargs):
        resource = scopes[0].replace("/.default", "") if scopes else "https://storage.azure.com"
        token = mssparkutils.credentials.getToken(resource)
        return AccessToken(token=token, expires_on=0)


_credential = _FabricTokenCredential()

# ---------------------------------------------------------------------------
# Secret retrieval
# ---------------------------------------------------------------------------


def get_secret(key_vault_name: str, secret_name: str) -> str:
    """Fetch a secret from Azure Key Vault via mssparkutils."""
    kv_url = f"https://{key_vault_name}.vault.azure.net/"
    return mssparkutils.credentials.getSecret(kv_url, secret_name)


# ---------------------------------------------------------------------------
# OneLake / ADLS Gen2 helpers
# ---------------------------------------------------------------------------


def onelake_abfss(workspace_name: str, lakehouse_name: str, path: str = "") -> str:
    """
    Build an ABFS URI for a path inside a Fabric Lakehouse.
    Example: abfss://fabric-dev-workspace@onelake.dfs.fabric.microsoft.com/fabric_lakehouse.lakehouse/Files/bronze/fedex/
    """
    host = "onelake.dfs.fabric.microsoft.com"
    item = f"{lakehouse_name}.lakehouse"
    base = f"abfss://{workspace_name}@{host}/{item}"
    return f"{base}/{path.lstrip('/')}" if path else base


def get_datalake_client(workspace_name: str):
    """Return an authenticated DataLakeServiceClient for OneLake."""
    account_url = "https://onelake.dfs.fabric.microsoft.com"
    return DataLakeServiceClient(account_url=account_url, credential=_credential)


def list_files(
    workspace_name: str,
    lakehouse_name: str,
    prefix: str,
    pattern: str = "*",
) -> list[str]:
    """
    List files under a OneLake prefix matching a glob pattern.
    Returns a list of full ABFS paths.
    """
    client = get_datalake_client(workspace_name)
    fs_name = workspace_name
    item_prefix = f"{lakehouse_name}.lakehouse/Files/{prefix.lstrip('/')}"

    fs_client = client.get_file_system_client(fs_name)
    paths = fs_client.get_paths(path=item_prefix, recursive=False)

    results = []
    for p in paths:
        name = p.name.split("/")[-1]
        if fnmatch.fnmatch(name, pattern):
            results.append(f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/{p.name}")
    return results


def read_file_bytes(abfss_path: str) -> bytes:
    """Download a file from OneLake and return raw bytes."""
    client = DataLakeFileClient.from_data_lake_url(abfss_path, credential=_credential)
    downloader = client.download_file()
    return downloader.readall()


def upload_file_bytes(
    workspace_name: str,
    lakehouse_name: str,
    dest_path: str,
    data: bytes,
    overwrite: bool = True,
) -> str:
    """Upload bytes to a OneLake path. Returns the ABFS path written."""
    account_url = "https://onelake.dfs.fabric.microsoft.com"
    client = DataLakeServiceClient(account_url=account_url, credential=_credential)

    fs_client = client.get_file_system_client(workspace_name)
    full_path = f"{lakehouse_name}.lakehouse/Files/{dest_path.lstrip('/')}"
    file_client = fs_client.get_file_client(full_path)
    file_client.upload_data(data, overwrite=overwrite)

    return f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/{full_path}"


def delete_path(workspace_name: str, lakehouse_name: str, path: str) -> None:
    """Delete a file or directory from OneLake."""
    account_url = "https://onelake.dfs.fabric.microsoft.com"
    client = DataLakeServiceClient(account_url=account_url, credential=_credential)
    fs_client = client.get_file_system_client(workspace_name)
    full_path = f"{lakehouse_name}.lakehouse/Files/{path.lstrip('/')}"
    fs_client.delete_directory(full_path)


# ---------------------------------------------------------------------------
# Fabric Data Warehouse helpers
# ---------------------------------------------------------------------------


def query_to_records(table: str, query: str) -> list[dict]:
    """Run a SELECT against a Fabric DW via synapsesql passthrough query.

    Uses the Fabric Spark connector pattern:
        spark.read.option(Constants.DatabaseName, "<warehouse>").synapsesql("<T-SQL>")
    See: https://learn.microsoft.com/en-us/fabric/data-engineering/spark-data-warehouse-connector
    """
    spark = SparkSession.builder.getOrCreate()
    db = table.split(".")[0]
    return [row.asDict() for row in spark.read.option(Constants.DatabaseName, db).synapsesql(query).collect()]



# ---------------------------------------------------------------------------
# Column name normalisation
# ---------------------------------------------------------------------------


def normalize_column_name(name: str) -> str:
    """Convert CamelCase / PascalCase / spaces to snake_case."""
    name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    name = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", name)
    name = name.replace(" ", "_").replace("-", "_")
    name = re.sub(r"_+", "_", name)
    return name.lower().strip("_")


# ---------------------------------------------------------------------------
# PySpark DataFrame helpers
# ---------------------------------------------------------------------------

# Java SimpleDateFormat patterns (used by PySpark to_date)
_DATE_FORMATS_SPARK = [
    "yyyy-MM-dd",
    "MM/dd/yyyy",
    "dd/MM/yyyy",
    "dd-MM-yyyy",
    "dd-MMM-yyyy",
    "yyyyMMdd",
]

# Translate Python strftime → Java SimpleDateFormat for callers using strftime strings
_STRFTIME_TO_SPARK: dict[str, str] = {
    "%Y-%m-%d": "yyyy-MM-dd",
    "%m/%d/%Y": "MM/dd/yyyy",
    "%d/%m/%Y": "dd/MM/yyyy",
    "%d-%m-%Y": "dd-MM-yyyy",
    "%d-%b-%Y": "dd-MMM-yyyy",
    "%Y%m%d":   "yyyyMMdd",
}


def spark_normalize_columns(sdf):
    """Rename all Spark DataFrame columns to snake_case."""
    return sdf.toDF(*[normalize_column_name(c) for c in sdf.columns])


def spark_trim_strings(sdf):
    """Strip leading/trailing whitespace from every string column."""
    str_cols = [f.name for f in sdf.schema.fields if isinstance(f.dataType, StringType)]
    for c in str_cols:
        sdf = sdf.withColumn(c, F.trim(F.col(c)))
    return sdf


def spark_cast_booleans_to_int(sdf):
    """Convert Boolean columns to Integer (0/1) for DW compatibility."""
    bool_cols = [f.name for f in sdf.schema.fields if isinstance(f.dataType, BooleanType)]
    for c in bool_cols:
        sdf = sdf.withColumn(c, sdf[c].cast("int"))
    return sdf


def spark_truncate_strings(sdf, max_len: int = 65535):
    """Truncate string columns exceeding max_len characters."""
    str_cols = [f.name for f in sdf.schema.fields if isinstance(f.dataType, StringType)]
    for c in str_cols:
        sdf = sdf.withColumn(c, F.col(c).substr(1, max_len))
    return sdf


def spark_extract_numeric(sdf, cols: list[str]):
    """Strip currency symbols, commas, spaces and cast to Double.
    Example: '$1,234.56' → 1234.56,  'N/A' → null"""
    for c in cols:
        if c not in sdf.columns:
            log.warning("spark_extract_numeric: column '%s' not found — skipping", c)
            continue
        sdf = sdf.withColumn(
            c,
            F.regexp_replace(F.col(c).cast("string"), r"[\$,\s%]", "").cast("double"),
        )
    return sdf


def spark_parse_dates(sdf, cols: list[str], fmt: str | None = None):
    """Parse string columns to Date in a Spark DataFrame.

    Args:
        fmt: strftime format (e.g. ``"%m/%d/%Y"``) or Java SimpleDateFormat
             (e.g. ``"MM/dd/yyyy"``). When None, each format in
             ``_DATE_FORMATS_SPARK`` is tried; the best match wins.
    """
    for c in cols:
        if c not in sdf.columns:
            log.warning("spark_parse_dates: column '%s' not found — skipping", c)
            continue
        if fmt is not None:
            spark_fmt = _STRFTIME_TO_SPARK.get(fmt, fmt)
            sdf = sdf.withColumn(c, F.to_date(F.col(c).cast("string"), spark_fmt))
        else:
            best_fmt, best_count = None, 0
            for candidate in _DATE_FORMATS_SPARK:
                try:
                    count = sdf.filter(
                        F.to_date(F.col(c).cast("string"), candidate).isNotNull()
                    ).count()
                    if count > best_count:
                        best_fmt, best_count = candidate, count
                except Exception:
                    continue
            if best_fmt:
                sdf = sdf.withColumn(c, F.to_date(F.col(c).cast("string"), best_fmt))
            else:
                log.warning("spark_parse_dates: no matching format for '%s'", c)
    return sdf


def spark_cast_to_schema(sdf, schema):
    """Coerce Spark DataFrame columns to match a StructType schema.
    Columns absent from sdf are ignored; uncastable values become null.

    Args:
        schema: ``pyspark.sql.types.StructType`` from an existing Delta table.
    """
    for field in schema.fields:
        if field.name in sdf.columns:
            try:
                sdf = sdf.withColumn(field.name, sdf[field.name].cast(field.dataType))
            except Exception as exc:
                log.warning("spark_cast_to_schema: cannot cast '%s' — %s", field.name, exc)
    return sdf


def spark_clean_dataframe(sdf):
    """Apply all standard PySpark cleaning: normalise columns, trim strings,
    cast booleans to int, truncate long strings."""
    sdf = spark_normalize_columns(sdf)
    sdf = spark_trim_strings(sdf)
    sdf = spark_cast_booleans_to_int(sdf)
    sdf = spark_truncate_strings(sdf)
    return sdf


def spark_apply_schema_cleaning(
    sdf,
    delta_path: str,
    date_format: str | None = None,
    timestamp_format: str | None = None,
):
    """Apply schema-driven cleaning based on the destination Delta table.

    Derives amount_cols, date_cols, and timestamp_cols from the existing Delta
    schema instead of requiring callers to hardcode them. Returns (sdf, table_exists).

    Args:
        sdf:              Input Spark DataFrame (already basic-cleaned).
        delta_path:       ABFSS path to the destination Delta table.
        date_format:      Optional strftime or Java date format for DateType columns.
        timestamp_format: Optional Java date format for TimestampType columns (default: yyyy-MM-dd HH:mm:ss.SSSSSS).

    Returns:
        (sdf, table_exists): cleaned DataFrame and bool indicating if table existed.
    """
    from pyspark.sql.types import DoubleType, FloatType, DecimalType, DateType, TimestampType

    spark = SparkSession.builder.getOrCreate()
    try:
        existing_schema = spark.read.format("delta").load(delta_path).schema
        table_exists = True

        amount_cols = [
            f.name for f in existing_schema.fields
            if isinstance(f.dataType, (DoubleType, FloatType, DecimalType))
            and f.name in sdf.columns
        ]
        date_cols = [
            f.name for f in existing_schema.fields
            if isinstance(f.dataType, DateType)
            and f.name in sdf.columns
        ]
        timestamp_cols = [
            f.name for f in existing_schema.fields
            if isinstance(f.dataType, TimestampType)
            and f.name in sdf.columns
        ]

        if amount_cols:
            sdf = spark_extract_numeric(sdf, amount_cols)
            log.info("spark_apply_schema_cleaning: extracted numeric %s", amount_cols)

        if date_cols:
            sdf = spark_parse_dates(sdf, date_cols, fmt=date_format)
            log.info("spark_apply_schema_cleaning: parsed dates %s (fmt=%s)", date_cols, date_format or "auto")

        if timestamp_cols:
            sdf = spark_parse_dates(sdf, timestamp_cols, fmt=timestamp_format)
            log.info("spark_apply_schema_cleaning: parsed timestamps %s (fmt=%s)", timestamp_cols, timestamp_format or "auto")

        sdf = spark_cast_to_schema(sdf, existing_schema)
        log.info("spark_apply_schema_cleaning: cast to schema (%d columns)", len(existing_schema.fields))
    except Exception:
        table_exists = False

    return sdf, table_exists


# ---------------------------------------------------------------------------
# Run logging
# ---------------------------------------------------------------------------


def log_run(
    log_table: str,
    source_name: str,
    layer: str,
    status: str,
    rows_processed: int = 0,
    file_name: str | None = None,
    error_message: str | None = None,
    run_id: str | None = None,
) -> None:
    """Insert a row into data_ops.pipeline_run_log via synapsesql."""
    schema = StructType([
        StructField("log_id",         LongType(),      False),
        StructField("source_name",    StringType(),    False),
        StructField("layer",          StringType(),    False),
        StructField("status",         StringType(),    False),
        StructField("rows_processed", IntegerType(),   True),
        StructField("file_name",      StringType(),    True),
        StructField("error_message",  StringType(),    True),
        StructField("run_id",         StringType(),    True),
        StructField("logged_at",      TimestampType(), True),
    ])
    log_id = int(time.time() * 1000000) + random.randint(0, 999999)
    now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    row = [(log_id, source_name, layer, status, int(rows_processed),
            file_name, error_message, run_id, now)]
    spark = SparkSession.builder.getOrCreate()
    sdf = spark.createDataFrame(row, schema)
    sdf.write.mode("append").synapsesql(log_table)
