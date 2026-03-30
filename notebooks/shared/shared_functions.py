"""
shared_functions.py — Shared helper library for Fabric notebooks.
  - azure-storage-file-datalake  (OneLake)
  - pyodbc                       (Fabric Data Warehouse)
  - mssparkutils.credentials     (Azure Key Vault)
"""
from __future__ import annotations

import fnmatch
import logging
import re
import struct
from collections import namedtuple
from contextlib import contextmanager
from typing import Iterator

import pyodbc
from notebookutils import mssparkutils

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
    Example: abfss://dev-fabric-data@onelake.dfs.fabric.microsoft.com/fabric_lakehouse.lakehouse/Files/bronze/fedex/
    """
    host = "onelake.dfs.fabric.microsoft.com"
    item = f"{lakehouse_name}.lakehouse"
    base = f"abfss://{workspace_name}@{host}/{item}"
    return f"{base}/{path.lstrip('/')}" if path else base


def get_datalake_client(workspace_name: str):
    """Return an authenticated DataLakeServiceClient for OneLake."""
    from azure.storage.filedatalake import DataLakeServiceClient

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
    from azure.storage.filedatalake import DataLakeFileClient

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
    from azure.storage.filedatalake import DataLakeServiceClient

    account_url = "https://onelake.dfs.fabric.microsoft.com"
    client = DataLakeServiceClient(account_url=account_url, credential=_credential)

    fs_client = client.get_file_system_client(workspace_name)
    full_path = f"{lakehouse_name}.lakehouse/Files/{dest_path.lstrip('/')}"
    file_client = fs_client.get_file_client(full_path)
    file_client.upload_data(data, overwrite=overwrite)

    return f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/{full_path}"


def delete_path(workspace_name: str, lakehouse_name: str, path: str) -> None:
    """Delete a file or directory from OneLake."""
    from azure.storage.filedatalake import DataLakeServiceClient

    account_url = "https://onelake.dfs.fabric.microsoft.com"
    client = DataLakeServiceClient(account_url=account_url, credential=_credential)
    fs_client = client.get_file_system_client(workspace_name)
    full_path = f"{lakehouse_name}.lakehouse/Files/{path.lstrip('/')}"
    fs_client.delete_directory(full_path)


# ---------------------------------------------------------------------------
# Fabric Data Warehouse connection
# ---------------------------------------------------------------------------


def get_dw_connection(sql_endpoint: str, database: str) -> pyodbc.Connection:
    """Open a pyodbc connection to a Fabric Data Warehouse using mssparkutils token."""
    token = _credential.get_token("https://database.windows.net/.default")

    token_bytes = token.token.encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)

    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={sql_endpoint};"
        f"DATABASE={database};"
        f"Encrypt=yes;TrustServerCertificate=no;"
    )
    conn = pyodbc.connect(conn_str, attrs_before={1256: token_struct})
    conn.autocommit = False
    return conn


@contextmanager
def dw_connection(sql_endpoint: str, database: str) -> Iterator[pyodbc.Connection]:
    """Context manager for a Fabric DW connection — commits on exit, rolls back on error."""
    conn = get_dw_connection(sql_endpoint, database)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def execute_sql(conn: pyodbc.Connection, sql: str, params: tuple = ()) -> None:
    """Execute a DML statement (INSERT/UPDATE/MERGE/EXEC) against Fabric DW."""
    cursor = conn.cursor()
    cursor.execute(sql, params)
    cursor.close()


def query_to_records(conn: pyodbc.Connection, sql: str) -> list[dict]:
    """Run a SELECT and return a list of dicts (one per row)."""
    cursor = conn.cursor()
    cursor.execute(sql)
    cols = [col[0] for col in cursor.description]
    rows = cursor.fetchall()
    cursor.close()
    return [dict(zip(cols, row)) for row in rows]


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
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType
    str_cols = [f.name for f in sdf.schema.fields if isinstance(f.dataType, StringType)]
    for c in str_cols:
        sdf = sdf.withColumn(c, F.trim(F.col(c)))
    return sdf


def spark_cast_booleans_to_int(sdf):
    """Convert Boolean columns to Integer (0/1) for DW compatibility."""
    from pyspark.sql.types import BooleanType
    bool_cols = [f.name for f in sdf.schema.fields if isinstance(f.dataType, BooleanType)]
    for c in bool_cols:
        sdf = sdf.withColumn(c, sdf[c].cast("int"))
    return sdf


def spark_truncate_strings(sdf, max_len: int = 65535):
    """Truncate string columns exceeding max_len characters."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType
    str_cols = [f.name for f in sdf.schema.fields if isinstance(f.dataType, StringType)]
    for c in str_cols:
        sdf = sdf.withColumn(c, F.col(c).substr(1, max_len))
    return sdf


def spark_extract_numeric(sdf, cols: list[str]):
    """Strip currency symbols, commas, spaces and cast to Double.
    Example: '$1,234.56' → 1234.56,  'N/A' → null"""
    from pyspark.sql import functions as F
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
    from pyspark.sql import functions as F
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


# ---------------------------------------------------------------------------
# Run logging
# ---------------------------------------------------------------------------


def log_run(
    conn: pyodbc.Connection,
    source_name: str,
    layer: str,
    status: str,
    rows_processed: int = 0,
    error_message: str | None = None,
    run_id: str | None = None,
) -> None:
    """Insert a row into data_ops.pipeline_run_log."""
    execute_sql(
        conn,
        """
        INSERT INTO data_ops.pipeline_run_log
            (source_name, layer, status, rows_processed, error_message, run_id, logged_at)
        VALUES (?, ?, ?, ?, ?, ?, GETUTCDATE())
        """,
        (source_name, layer, status, rows_processed, error_message, run_id),
    )
