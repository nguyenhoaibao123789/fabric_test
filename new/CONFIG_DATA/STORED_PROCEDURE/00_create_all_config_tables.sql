/* ============================================================
   MASTER DDL — mdf_platform_orchestration config tables
   Fabric Warehouse (T-SQL) — fully idempotent

   Run this script ONCE per environment to bootstrap all
   config tables. Re-running is safe (IF OBJECT_ID guards).

   Execution order matters:
     1. Schema
     2. elt_table_config          (parent — no FK deps)
     3. elt_schema_config         (FK → elt_table_config)
     4. elt_transform_config      (FK → elt_table_config)
     5. elt_dq_config             (FK → elt_table_config)
     6. elt_dq_results            (no FK — audit log)

   Column note:
     [bronze_file_type] = pandas reader format passed to bronze2silver
     notebooks (parquet | csv | json | xml | excel). NULL for src2brz
     rows (DB→Parquet copy activities do not need a reader format).
   ============================================================ */


-- ── 0. Schema ──────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'mdf_platform_orchestration')
    EXEC('CREATE SCHEMA [mdf_platform_orchestration]');
GO


-- ── 1. elt_table_config ────────────────────────────────────────────────────────
-- One row per table per layer. Drives all pipeline decisions.
-- Routing key: layer + datasubject (extracted from pipeline_name by get_config pipelines).
IF OBJECT_ID(N'[mdf_platform_orchestration].[elt_table_config]', 'U') IS NULL
BEGIN
    CREATE TABLE [mdf_platform_orchestration].[elt_table_config] (

        [table_id]             INT             NOT NULL,
        [datasubject]          VARCHAR(100)    NOT NULL,   -- e.g. iccp-finance | pdo-finance | pdo-sales-marketing | sppi-operations
        [classification]       VARCHAR(50)     NOT NULL,   -- restricted | confidential | internal | public
        [sourcesystem]         VARCHAR(100)    NOT NULL,   -- sqlvm | sharepoint | sap | awsrds | sftp | api
        [sourceschema]         VARCHAR(100)    NULL,
        [sourceschemaname]     VARCHAR(100)    NULL,
        [sourcetablename]      VARCHAR(200)    NULL,
        [tablename]            VARCHAR(200)    NOT NULL,   -- bronze folder leaf AND silver Warehouse target table
        [job_group]            VARCHAR(100)    NULL,
        [ingest_channel]       VARCHAR(100)    NULL,
        [layer]                VARCHAR(50)     NOT NULL,   -- src2brz | bronze2silver | silver2gold | gold2platinum
        [container]            VARCHAR(50)     NULL,       -- bronze | silver | gold | platinum
        [sequence_number]      INT             NOT NULL,   -- batch ordering; lower runs first
        [cycle]                VARCHAR(50)     NULL,       -- daily | hourly | manual
        [process_id]           INT             NULL,
        [ref_table_id]         INT             NULL,       -- FK → src2brz row; NULL for src2brz rows
        [bronze_file_format]   VARCHAR(100)    NOT NULL,   -- sql_table | excel | csv | sharepoint_list | sap_table | rds_table
        [bronze_file_type]     VARCHAR(50)     NULL,       -- parquet | csv | json | xml | excel (pandas reader for bronze2silver)
        [file_pattern]         VARCHAR(200)    NULL,
        [custom_source_path]   VARCHAR(500)    NULL,
        [custom_table_name]    VARCHAR(200)    NULL,
        [ingest_partition]     VARCHAR(50)     NULL,
        [ref_ingest_partition] VARCHAR(50)     NULL,
        [last_loaded_dt]       DATETIME2       NULL,
        [criteria_columns]     VARCHAR(500)    NULL,       -- comma-sep business-key cols for SCD2 merge
        [full_refresh_flag]    CHAR(1)         NOT NULL    -- Y = TRUNCATE+INSERT; N = SCD2 / append
            CONSTRAINT [df_full_refresh_flag] DEFAULT ('N'),

        CONSTRAINT [PK_elt_table_config] PRIMARY KEY ([table_id])
    );
END;
GO


-- ── 2. elt_schema_config ───────────────────────────────────────────────────────
-- Column-level mapping for DB sources (sql_table / sap_table / rds_table).
-- src2brz uses this to build TabularTranslator JSON.
-- bronze2silver Cell 4b uses this to rename and cast columns.
IF OBJECT_ID(N'[mdf_platform_orchestration].[elt_schema_config]', 'U') IS NULL
BEGIN
    CREATE TABLE [mdf_platform_orchestration].[elt_schema_config] (

        [table_id]               INT              NOT NULL,
        [tableschema]            VARCHAR(100)     NOT NULL,
        [tablename]              VARCHAR(200)     NOT NULL,
        [source_column_position] INT              NOT NULL,
        [source_column_name]     VARCHAR(200)     NOT NULL,
        [source_data_type]       VARCHAR(50)      NOT NULL,
        [target_column_name]     VARCHAR(200)     NOT NULL,
        [target_data_type]       VARCHAR(50)      NOT NULL,

        CONSTRAINT [PK_elt_schema_config] PRIMARY KEY ([table_id], [source_column_position]),
        CONSTRAINT [FK_elt_schema_config_table] FOREIGN KEY ([table_id])
            REFERENCES [mdf_platform_orchestration].[elt_table_config] ([table_id])
    );
END;
GO


-- ── 3. elt_transform_config ────────────────────────────────────────────────────
-- Per-table transform rules; applied by bronze2silver_transform_nbk Cell 4c.
-- Supported transform_type values:
--   add_literal_column | remove_column | filter_rows | dedup
--   split_datetime | union_table | unpivot
IF OBJECT_ID(N'[mdf_platform_orchestration].[elt_transform_config]', 'U') IS NULL
BEGIN
    CREATE TABLE [mdf_platform_orchestration].[elt_transform_config] (

        [rule_id]         INT           NOT NULL,
        [table_id]        INT           NOT NULL,
        [transform_type]  VARCHAR(50)   NOT NULL,
        [source_column]   VARCHAR(200)  NULL,
        [target_column]   VARCHAR(200)  NULL,
        [rule_value]      VARCHAR(500)  NULL,
        [sequence_number] INT           NOT NULL,

        CONSTRAINT [PK_elt_transform_config] PRIMARY KEY ([rule_id]),
        CONSTRAINT [FK_elt_transform_config_table] FOREIGN KEY ([table_id])
            REFERENCES [mdf_platform_orchestration].[elt_table_config] ([table_id])
    );
END;
GO


-- ── 4. elt_dq_config ───────────────────────────────────────────────────────────
-- Per-column DQ rules; evaluated by bronze2silver_dq_nbk after silver load.
-- Supported rule_type values:  allowed_values | not_null | range | regex
-- Supported action values:     warn | flag | reject
IF OBJECT_ID(N'[mdf_platform_orchestration].[elt_dq_config]', 'U') IS NULL
BEGIN
    CREATE TABLE [mdf_platform_orchestration].[elt_dq_config] (

        [rule_id]     INT           NOT NULL,
        [table_id]    INT           NOT NULL,
        [column_name] VARCHAR(200)  NOT NULL,
        [rule_type]   VARCHAR(50)   NOT NULL,
        [rule_value]  VARCHAR(500)  NULL,
        [action]      VARCHAR(20)   NOT NULL
            CONSTRAINT [df_dq_action] DEFAULT ('warn'),
        [is_active]   BIT           NOT NULL
            CONSTRAINT [df_dq_active] DEFAULT (1),

        CONSTRAINT [PK_elt_dq_config] PRIMARY KEY ([rule_id]),
        CONSTRAINT [FK_elt_dq_config_table] FOREIGN KEY ([table_id])
            REFERENCES [mdf_platform_orchestration].[elt_table_config] ([table_id])
    );
END;
GO


-- ── 5. elt_dq_results ──────────────────────────────────────────────────────────
-- Audit log written by bronze2silver_dq_nbk after each pipeline run.
IF OBJECT_ID(N'[mdf_platform_orchestration].[elt_dq_results]', 'U') IS NULL
BEGIN
    CREATE TABLE [mdf_platform_orchestration].[elt_dq_results] (

        [result_id]         INT IDENTITY  NOT NULL,
        [pipeline_run_id]   VARCHAR(100)  NOT NULL,
        [table_id]          INT           NOT NULL,
        [tablename]         VARCHAR(200)  NOT NULL,
        [column_name]       VARCHAR(200)  NOT NULL,
        [rule_type]         VARCHAR(50)   NOT NULL,
        [rule_value]        VARCHAR(500)  NULL,
        [action]            VARCHAR(20)   NOT NULL,
        [fail_count]        INT           NOT NULL DEFAULT 0,
        [total_count]       INT           NOT NULL DEFAULT 0,
        [pass_rate_pct]     DECIMAL(5,2)  NULL,
        [checked_at]        DATETIME2     NOT NULL DEFAULT GETUTCDATE(),

        CONSTRAINT [PK_elt_dq_results] PRIMARY KEY ([result_id])
    );
END;
GO
