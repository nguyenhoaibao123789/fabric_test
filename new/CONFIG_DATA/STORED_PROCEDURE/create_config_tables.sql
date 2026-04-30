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
    [layer]                 VARCHAR(max)      NOT NULL,   -- src2brz | bronze2silver | silver2gold | gold2platinum
    [datasubject]           VARCHAR(max)     NOT NULL,   -- e.g. iccp-finance | pdo-finance | pdo-sales-marketing | sppi-operations
    [sequence_number]       INT              NOT NULL,   -- processing wave; lower runs first
    [bronze_file_format]    VARCHAR(max)     NOT NULL,   -- sql_table | excel | csv | sharepoint_list | sap_table | rds_table
    [cycle]                 VARCHAR(max)      NOT NULL,   -- daily | hourly | manual

    -- Source identity
    [classification]        VARCHAR(max)      NOT NULL,   -- restricted | confidential | internal | public
    [sourcesystem]          VARCHAR(max)     NOT NULL,   -- sqlvm | sharepoint | sap | awsrds | sftp | api
    [sourceschema]          VARCHAR(max)     NULL,
    [sourceschemaname]      VARCHAR(max)     NULL,       -- used in SQL: SELECT * FROM [@sourceschemaname].[@tablename]
    [sourcetablename]       VARCHAR(max)     NULL,       -- source table / file name
    [tablename]             VARCHAR(max)     NOT NULL,   -- bronze folder leaf + Warehouse target table name

    -- Path construction (bronze Lakehouse)
    [container]             VARCHAR(max)      NULL,       -- bronze | silver | gold
    [ingest_channel]        VARCHAR(max)     NULL,       -- subfolder: ingest_channel=dfp

    -- File source columns (populated for excel | csv | sharepoint_list sources)
    [custom_source_path]    VARCHAR(max)     NULL,       -- SharePoint site URL or landing folder
    [bronze_file_type]      VARCHAR(max)      NULL,       -- parquet | csv | json | xml | excel → pandas reader (bronze2silver only)
    [file_pattern]          VARCHAR(max)     NULL,       -- wildcard for SharePoint doc lib (e.g. fct_sales* or exact filename)
    [custom_table_name]     VARCHAR(max)     NULL,       -- SharePoint list GUID (sharepoint_list sources only)

    -- Watermark — updated by update_load_status SP after each successful load
    [ingest_partition]      VARCHAR(max)      NULL,       -- last partition written: yyyy-MM-dd-HH
    [ref_ingest_partition]  VARCHAR(max)      NULL,
    [last_loaded_dt]        DATETIME2(6)     NULL,       -- derived from ingest_partition by SP (bronze2silver+)

    -- SCD2 / merge control (used by bronze2silver notebook)
    [criteria_columns]      VARCHAR(max)     NULL,       -- comma-separated business key cols for SCD2 merge
    [full_refresh_flag]     CHAR(1)          NOT NULL,    -- Y = TRUNCATE + bulk insert; N = SCD2 / append

    -- Orchestration
    [job_group]             VARCHAR(max)     NULL,
    [ref_table_id]          INT              NULL,       -- FK → src2brz row; NULL for src2brz rows
    [process_id]            INT              NULL
);
GO


-- ============================================================
-- elt_schema_config
-- Column mapping for DB sources (sql_table / sap_table / rds_table).
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

    CONSTRAINT [PK_elt_schema_config_ctc] PRIMARY KEY ([table_id], [source_column_position]),
    CONSTRAINT [FK_elt_schema_config_table_ctc] FOREIGN KEY ([table_id])
        REFERENCES [mdf_platform_orchestration].[elt_table_config] ([table_id])
);
GO
