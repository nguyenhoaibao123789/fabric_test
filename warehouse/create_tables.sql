-- =============================================================
-- Fabric Data Warehouse — DDL
-- Run once manually in the Fabric SQL editor
-- =============================================================

-- ── Schemas ────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS data_ops;
CREATE SCHEMA IF NOT EXISTS gold;

-- =============================================================
-- OPERATIONAL TABLES (data_ops schema)
-- =============================================================

-- Pipeline run log — every Bronze/Silver/Gold task writes here
CREATE TABLE IF NOT EXISTS data_ops.pipeline_run_log (
    log_id          BIGINT          IDENTITY(1,1) PRIMARY KEY,
    source_name     VARCHAR(200)    NOT NULL,
    layer           VARCHAR(20)     NOT NULL,  -- bronze | silver1 | silver2 | gold
    status          VARCHAR(20)     NOT NULL,  -- success | failed
    rows_processed  INT             NULL,
    file_name       VARCHAR(500)    NULL,
    error_message   VARCHAR(4000)   NULL,
    run_id          VARCHAR(500)    NULL,
    logged_at       DATETIME2       NOT NULL DEFAULT GETUTCDATE()
);

CREATE INDEX ix_run_log_source_layer ON data_ops.pipeline_run_log (source_name, layer, status);
CREATE INDEX ix_run_log_logged_at    ON data_ops.pipeline_run_log (logged_at);

-- =============================================================
-- GOLD TABLES — dim_carrier
-- Fed by: medallion_rate_card → silver_carrier_rate_card
-- =============================================================

CREATE TABLE IF NOT EXISTS gold.dim_carrier (
    carrier_key     INT             IDENTITY(1,1) PRIMARY KEY,
    carrier_code    VARCHAR(50)     NOT NULL,
    carrier_name    VARCHAR(200)    NULL,
    is_active       BIT             NOT NULL DEFAULT 1,
    created_at      DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    updated_at      DATETIME2       NOT NULL DEFAULT GETUTCDATE()
);

CREATE UNIQUE INDEX uix_dim_carrier_code ON gold.dim_carrier (carrier_code);
