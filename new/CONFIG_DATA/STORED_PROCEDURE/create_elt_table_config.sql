/* ============================================================
   CREATE TABLE  mdf_platform_orchestration.elt_table_config
   Fabric Warehouse (T-SQL)
   ============================================================ */

CREATE TABLE [mdf_platform_orchestration].[elt_table_config] (

    -- ── Identity ───────────────────────────────────────────────
    [table_id]             INT             NOT NULL,   -- surrogate key; assign manually per source row

    -- ── Source location ────────────────────────────────────────
    [datasubject]          VARCHAR(100)    NOT NULL,   -- e.g. finance, warehouse, sales
    [classification]       VARCHAR(50)     NOT NULL,   -- confidential | regulated | restricted | sensitive
    [sourcesystem]         VARCHAR(100)    NOT NULL,   -- e.g. sqlvm, sharepoint, sap
    [sourceschema]         VARCHAR(100)    NULL,        -- folder segment in bronze path
    [sourceschemaname]     VARCHAR(100)    NULL,        -- schema name passed to SQL query (DB sources only)
    [sourcetablename]      VARCHAR(200)    NULL,        -- source table name (DB) or file name hint (file sources)
    [tablename]            VARCHAR(200)    NOT NULL,   -- bronze folder leaf AND silver Warehouse target table

    -- ── Orchestration ──────────────────────────────────────────
    [job_group]            VARCHAR(100)    NULL,
    [ingest_channel]       VARCHAR(100)    NULL,        -- e.g. ingest_channel=dfp
    [layer]                VARCHAR(50)     NOT NULL,   -- src2brz | bronze2silver | silver2gold | gold2platinum
    [container]            VARCHAR(50)     NULL,        -- bronze | silver | gold | platinum
    [sequence_number]         INT             NOT NULL,   -- batch ordering (10, 20, 30 …); lower runs first
    [cycle]                VARCHAR(50)     NULL,        -- daily | hourly | manual
    [process_id]           INT             NULL,        -- legacy process reference
    [ref_table_id]         INT             NULL,        -- FK → src2brz row; set for bronze2silver+ rows; NULL for src2brz

    -- ── Routing (Switch discriminator) ────────────────────────
    [bronze_file_format]   VARCHAR(100)    NOT NULL,   -- parquet_none_header | file_as_is | sharepoint_list | sap_table
                                                        -- | csv | parquet | json | excel

    -- ── File source config ────────────────────────────────────
    [file_type]            VARCHAR(50)     NULL,        -- parquet | csv | json | excel — tells B2S which pandas reader to use
    [bronze_file_type]     VARCHAR(50)     NULL,        -- physical format of the bronze file (parquet | delta | binary)
    [file_pattern]         VARCHAR(200)    NULL,        -- wildcard for SharePoint doc lib (e.g. fct_sales* or exact filename)
    [custom_source_path]   VARCHAR(500)    NULL,        -- SharePoint site URL (file_as_is / sharepoint_list) or landing folder
    [custom_table_name]    VARCHAR(200)    NULL,        -- SharePoint list ID (sharepoint_list sources only)

    -- ── Watermark ──────────────────────────────────────────────
    [ingest_partition]     VARCHAR(50)     NULL,        -- last partition successfully loaded; format yyyy-MM-dd-HH
    [ref_ingest_partition] VARCHAR(50)     NULL,        -- reference partition (cross-layer linkage)
    [last_loaded_dt]       DATETIME2       NULL,        -- timestamp of last successful load (updated by update_load_status SP)

    -- ── SCD2 / merge control ──────────────────────────────────
    [criteria_columns]     VARCHAR(500)    NULL,        -- comma-separated business-key columns for SCD2 merge
    [full_refresh_flag]    CHAR(1)         NOT NULL     -- 1 = TRUNCATE+INSERT (full reload); 0 = SCD2 / append
        CONSTRAINT [df_full_refresh_flag] DEFAULT ('0')
);
