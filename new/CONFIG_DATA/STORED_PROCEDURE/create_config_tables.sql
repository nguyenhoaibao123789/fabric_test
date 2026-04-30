-- ============================================================
-- Schema
-- ============================================================
IF NOT EXISTS (
    SELECT 1 FROM sys.schemas WHERE name = 'mdf_platform_orchestration'
)
    EXEC('CREATE SCHEMA [mdf_platform_orchestration]');
GO


-- ============================================================
-- elt_table_config
-- One row per table per layer. Drives all pipeline decisions.
-- ============================================================
CREATE TABLE [mdf_platform_orchestration].[elt_table_config] (

    -- Identity
    [table_id]              INT              NOT NULL,

    -- Routing — coordinator + get_config use these to filter rows
    [layer]                 VARCHAR(50)      NOT NULL,   -- src2brz | bronze2silver | silver2gold | gold2platinum
    [datasubject]           VARCHAR(100)     NOT NULL,   -- application | sales | warehouse | purchasing
    [sequence_number]          INT              NOT NULL,   -- processing wave; lower runs first
    [stg_file_format]       VARCHAR(50)      NOT NULL,   -- parquet_none_header | csv | parquet | json | excel
    [cycle]                 VARCHAR(50)      NOT NULL,   -- daily | hourly | manual

    -- Source identity
    [classification]        VARCHAR(50)      NOT NULL,   -- confidential | restricted | sensitive | regulated
    [sourcesystem]          VARCHAR(100)     NOT NULL,   -- sqlvm | sftp | api …
    [sourceschema]          VARCHAR(100)     NOT NULL,
    [sourceschemaname]      VARCHAR(100)     NOT NULL,   -- used in SQL: SELECT * FROM [@sourceschemaname].[@tablename]
    [sourcetablename]       VARCHAR(200)     NOT NULL,   -- source table / file name
    [tablename]             VARCHAR(200)     NOT NULL,   -- bronze folder leaf + Warehouse target table name

    -- Path construction (bronze Lakehouse)
    [container]             VARCHAR(50)      NOT NULL,   -- bronze (src2brz rows)
    [ingest_channel]        VARCHAR(100)     NOT NULL,   -- subfolder: ingest_channel=dfp

    -- File source columns (populated when stg_file_format != parquet_none_header)
    [custom_source_path]    VARCHAR(500)     NULL,       -- landing zone folder in Lakehouse Files/
    [file_type]             VARCHAR(50)      NULL,       -- parquet | csv | json | xml | excel → pandas reader

    -- Watermark — updated by update_load_status SP after each successful load
    [ingest_partition]      VARCHAR(20)      NULL,       -- last partition written: yyyy-MM-dd-HH
    [last_loaded_dt]        DATETIME2(7)     NULL,       -- derived from ingest_partition by SP (bronze2silver+)

    -- SCD2 / merge control (used by bronze2silver notebook)
    [criteria_columns]      VARCHAR(500)     NULL,       -- comma-separated business key cols for SCD2 merge
    [full_refresh_flag]     INT              NOT NULL,   -- 1 = TRUNCATE + bulk insert; 0 = SCD2 merge

    -- Legacy columns (kept for compatibility)
    [job_group]             VARCHAR(100)     NULL,
    [custom_table_name]     VARCHAR(200)     NULL,
    [stg_file_type]         VARCHAR(50)      NULL,
    [ref_table_id]          INT              NULL,
    [process_id]            INT              NOT NULL,
    [ref_ingest_partition]  VARCHAR(20)      NULL,

    CONSTRAINT [PK_elt_table_config] PRIMARY KEY ([table_id])
);
GO


-- ============================================================
-- elt_schema_config
-- Column mapping for DB sources (parquet_none_header).
-- Used by src2brz_copy_from_sql_to_parquet to build the
-- TabularTranslator JSON for the Copy activity.
-- ============================================================
CREATE TABLE [mdf_platform_orchestration].[elt_schema_config] (

    [table_id]               INT              NOT NULL,   -- FK to elt_table_config.table_id
    [tableschema]            VARCHAR(100)     NOT NULL,
    [tablename]              VARCHAR(200)     NOT NULL,
    [source_column_position] INT              NOT NULL,   -- column ordinal; drives Parquet column order
    [source_column_name]     VARCHAR(200)     NOT NULL,
    [source_data_type]       VARCHAR(50)      NOT NULL,   -- SQL Server type: nvarchar | int | datetime2 | decimal …
    [target_column_name]     VARCHAR(200)     NOT NULL,
    [target_data_type]       VARCHAR(50)      NOT NULL,   -- Parquet type: STRING | INT32 | INT64 | INT96 | DECIMAL | BOOLEAN | BYTE_ARRAY

    CONSTRAINT [PK_elt_schema_config] PRIMARY KEY ([table_id], [source_column_position]),
    CONSTRAINT [FK_elt_schema_config_table] FOREIGN KEY ([table_id])
        REFERENCES [mdf_platform_orchestration].[elt_table_config] ([table_id])
);
GO
