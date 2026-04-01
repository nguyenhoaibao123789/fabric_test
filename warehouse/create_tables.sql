-- =============================================================
-- Fabric Data Warehouse — DDL
-- Run once manually in the Fabric SQL editor
-- =============================================================

-- ── Schemas ────────────────────────────────────────────────
-- Fabric DW uses T-SQL but does not support IF NOT EXISTS on
-- CREATE SCHEMA / CREATE TABLE.  It also does not support
-- user-created indexes, PRIMARY KEY constraints, or BIT type.

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'data_ops')
    EXEC('CREATE SCHEMA data_ops');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
    EXEC('CREATE SCHEMA gold');
GO

-- =============================================================
-- OPERATIONAL TABLES (data_ops schema)
-- =============================================================

-- Pipeline run log — every Bronze/Silver/Gold task writes here
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES
               WHERE TABLE_SCHEMA = 'data_ops' AND TABLE_NAME = 'pipeline_run_log')
CREATE TABLE data_ops.pipeline_run_log (
    log_id          BIGINT          NOT NULL,
    source_name     VARCHAR(200)    NOT NULL,
    layer           VARCHAR(20)     NOT NULL,  -- bronze | silver1 | silver2 | gold
    status          VARCHAR(20)     NOT NULL,  -- success | failed
    rows_processed  INT             NULL,
    file_name       VARCHAR(500)    NULL,
    error_message   VARCHAR(4000)   NULL,
    run_id          VARCHAR(500)    NULL,
    logged_at       DATETIME2(6)    NULL        -- set to GETUTCDATE() at insert time
);
GO

-- Note: Fabric DW does not support CREATE INDEX or DEFAULT constraints.
-- Query performance is managed automatically by the engine.
-- Default values must be applied at insert time (e.g. in shared_functions.log_run).

-- =============================================================
-- GOLD TABLES — dim_carrier
-- Fed by: medallion_rate_card → silver_carrier_rate_card
-- =============================================================

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES
               WHERE TABLE_SCHEMA = 'gold' AND TABLE_NAME = 'dim_carrier')
CREATE TABLE gold.dim_carrier (
    carrier_key     BIGINT             IDENTITY NOT NULL,
    carrier_code    VARCHAR(50)     NOT NULL,
    carrier_name    VARCHAR(200)    NULL,
    is_active       INT             NULL,           -- 1 = active, 0 = inactive
    created_at      DATETIME2(6)    NULL,           -- set to GETUTCDATE() at insert time
    updated_at      DATETIME2(6)    NULL            -- set to GETUTCDATE() at insert time
);
GO
