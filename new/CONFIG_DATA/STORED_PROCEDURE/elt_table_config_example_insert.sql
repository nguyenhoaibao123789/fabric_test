/* ============================================================
   EXAMPLE INSERT — elt_table_config
   Two paired rows: src2brz (id=23) + bronze2silver (id=54)
   for warehouse.coldroomtemperatures via SQL Server on-prem
   ============================================================ */

INSERT INTO [mdf_platform_orchestration].[elt_table_config] (
    [table_id], [datasubject], [classification], [sourcesystem],
    [sourceschema], [sourceschemaname], [sourcetablename], [tablename],
    [job_group], [ingest_channel], [layer], [container],
    [sequence_number], [cycle], [process_id], [ref_table_id],
    [bronze_file_format], [file_type], [bronze_file_type],
    [file_pattern], [custom_source_path], [custom_table_name],
    [ingest_partition], [ref_ingest_partition], [last_loaded_dt],
    [criteria_columns], [full_refresh_flag]
)
VALUES
/* ── src2brz row ──────────────────────────────────────────────
   Reads from on-prem SQL Server via gateway.
   Writes Parquet to Lakehouse Files/bronze/...
   ref_table_id = NULL (no upstream dependency)
   ──────────────────────────────────────────────────────────────── */
(
    23,                         -- table_id
    'warehouse',                -- datasubject
    'restricted',               -- classification
    'sqlvm',                    -- sourcesystem
    'warehouse',                -- sourceschema        (bronze folder segment)
    'warehouse',                -- sourceschemaname    (schema name in SQL query)
    'coldroomtemperatures',     -- sourcetablename     (source table name)
    'coldroomtemperatures',     -- tablename           (bronze folder leaf)
    NULL,                       -- job_group
    'ingest_channel=dfp',       -- ingest_channel
    'src2brz',               -- layer
    'bronze',                   -- container
    10,                         -- sequence_number        (runs in batch 1)
    'daily',                    -- cycle
    0,                          -- process_id
    NULL,                       -- ref_table_id        (NULL for src2brz)
    'parquet_none_header',      -- bronze_file_format  (Switch key → copy_from_sql_to_parquet)
    NULL,                       -- file_type           (N/A for DB source)
    'parquet',                  -- bronze_file_type    (physical format written to bronze)
    NULL,                       -- file_pattern        (N/A for DB source)
    NULL,                       -- custom_source_path  (N/A for DB source)
    NULL,                       -- custom_table_name   (N/A for DB source)
    '2025-09-29-08',            -- ingest_partition    (last successfully loaded partition)
    NULL,                       -- ref_ingest_partition
    NULL,                       -- last_loaded_dt
    NULL,                       -- criteria_columns    (no SCD2 key — full refresh)
    '1'                         -- full_refresh_flag   (1 = TRUNCATE+INSERT)
),

/* ── bronze2silver row ───────────────────────────────────────────
   Reads from the Parquet written by the src2brz row above.
   Writes to [silver].[coldroomtemperatures] in Fabric Warehouse.
   ref_table_id = 23  (links back to the src2brz row)
   ──────────────────────────────────────────────────────────────── */
(
    54,                         -- table_id
    'warehouse',                -- datasubject
    'restricted',               -- classification
    'sqlvm',                    -- sourcesystem
    'warehouse',                -- sourceschema
    'warehouse',                -- sourceschemaname
    'coldroomtemperatures',     -- sourcetablename
    'coldroomtemperatures',     -- tablename           (target: [silver].[coldroomtemperatures])
    NULL,                       -- job_group
    'ingest_channel=dfp',       -- ingest_channel
    'bronze2silver',            -- layer
    'silver',                   -- container
    10,                         -- sequence_number        (same batch as src2brz — both seq 1)
    'daily',                    -- cycle
    0,                          -- process_id
    23,                         -- ref_table_id        (→ src2brz row id=23)
    'parquet_none_header',      -- bronze_file_format  (tells notebook the bronze file is Parquet)
    'parquet',                  -- file_type           (pandas reader: pd.read_parquet)
    'delta',                    -- bronze_file_type    (target format in silver — delta/warehouse)
    NULL,                       -- file_pattern
    NULL,                       -- custom_source_path
    NULL,                       -- custom_table_name
    '2025-08-30-10',            -- ingest_partition    (last partition loaded to silver; B2S watermark)
    NULL,                       -- ref_ingest_partition
    NULL,                       -- last_loaded_dt
    NULL,                       -- criteria_columns    (no SCD2 key — full refresh)
    '1'                         -- full_refresh_flag   (1 = TRUNCATE+INSERT)
);
