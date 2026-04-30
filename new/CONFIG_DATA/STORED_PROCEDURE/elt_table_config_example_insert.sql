/* ============================================================
   EXAMPLE INSERT — elt_table_config
   Two paired rows: src2brz (id=1) + bronze2silver (id=101)
   for iccp-finance.iccp_income_statement via SharePoint Folder (Excel)
   ============================================================ */

INSERT INTO [mdf_platform_orchestration].[elt_table_config] (
    [table_id], [datasubject], [classification], [sourcesystem],
    [sourceschema], [sourceschemaname], [sourcetablename], [tablename],
    [job_group], [ingest_channel], [layer], [container],
    [sequence_number], [cycle], [process_id], [ref_table_id],
    [bronze_file_format], [bronze_file_type],
    [file_pattern], [custom_source_path], [custom_table_name],
    [ingest_partition], [ref_ingest_partition], [last_loaded_dt],
    [criteria_columns], [full_refresh_flag]
)
VALUES
/* ── src2brz row ──────────────────────────────────────────────
   Reads Excel file from SharePoint Folder via MDF_SHAREPOINT_DEV_001.
   Copies file as-is to Lakehouse Files/bronze/...
   ref_table_id = NULL (no upstream dependency)
   ──────────────────────────────────────────────────────────────── */
(
    1,                              -- table_id
    'iccp-finance',                 -- datasubject         (domain_businessunit)
    'restricted',                   -- classification      restricted | confidential | internal | public
    'sharepoint',                   -- sourcesystem
    'finance',                      -- sourceschema        (bronze folder segment)
    NULL,                           -- sourceschemaname    (NULL for file sources)
    'ICCP_IS_Dimension',            -- sourcetablename     (Power BI / SQL table name)
    'iccp_income_statement',        -- tablename           (bronze folder leaf + silver target)
    NULL,                           -- job_group
    'ingest_channel=dfp',           -- ingest_channel
    'src2brz',                      -- layer
    'bronze',                       -- container
    10,                             -- sequence_number     (runs in batch 1)
    'daily',                        -- cycle
    0,                              -- process_id
    NULL,                           -- ref_table_id        (NULL for src2brz)
    'excel',                        -- bronze_file_format  (Switch key → copy_file_as_is)
    NULL,                           -- bronze_file_type    (N/A for src2brz — no pandas reader needed)
    'PDO Income Statement Dimension.xlsx', -- file_pattern (exact filename or wildcard)
    'https://iccpgroup.sharepoint.com/sites/ICCPFinancialDataReports/Shared Documents/Financial Statements/PDO/FS Maintenance Files',
                                    -- custom_source_path  (SharePoint folder URL)
    NULL,                           -- custom_table_name   (N/A — not a SharePoint list)
    NULL,                           -- ingest_partition
    NULL,                           -- ref_ingest_partition
    NULL,                           -- last_loaded_dt
    NULL,                           -- criteria_columns    (no SCD2 key — full refresh)
    'Y'                             -- full_refresh_flag   (Y = TRUNCATE+INSERT)
),

/* ── bronze2silver row ───────────────────────────────────────────
   Reads the Excel file landed in bronze by the src2brz row above.
   Writes to [silver].[iccp_income_statement] in Fabric Warehouse.
   ref_table_id = 1  (links back to the src2brz row)
   ──────────────────────────────────────────────────────────────── */
(
    101,                            -- table_id
    'iccp-finance',                 -- datasubject
    'restricted',                   -- classification
    'sharepoint',                   -- sourcesystem
    'finance',                      -- sourceschema
    NULL,                           -- sourceschemaname
    'ICCP_IS_Dimension',            -- sourcetablename
    'iccp_income_statement',        -- tablename           (target: [silver].[iccp_income_statement])
    NULL,                           -- job_group
    'ingest_channel=dfp',           -- ingest_channel
    'bronze2silver',                -- layer
    'silver',                       -- container
    10,                             -- sequence_number
    'daily',                        -- cycle
    0,                              -- process_id
    1,                              -- ref_table_id        (→ src2brz row id=1)
    'excel',                        -- bronze_file_format  (Switch key → copy_file_as_is)
    'excel',                        -- bronze_file_type    (pandas reader: pd.read_excel)
    NULL,                           -- file_pattern
    NULL,                           -- custom_source_path
    NULL,                           -- custom_table_name
    NULL,                           -- ingest_partition
    NULL,                           -- ref_ingest_partition
    NULL,                           -- last_loaded_dt
    NULL,                           -- criteria_columns    (no SCD2 key — full refresh)
    'Y'                             -- full_refresh_flag   (Y = TRUNCATE+INSERT)
);
