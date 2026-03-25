-- =============================================================
-- Fabric Data Warehouse — DDL
-- Run once via deploy.py or manually in the Fabric SQL editor
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
-- GOLD DIM TABLES
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

CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key        INT             PRIMARY KEY,   -- YYYYMMDD
    full_date       DATE            NOT NULL,
    year            SMALLINT        NOT NULL,
    quarter         TINYINT         NOT NULL,
    month           TINYINT         NOT NULL,
    month_name      VARCHAR(20)     NOT NULL,
    week_of_year    TINYINT         NOT NULL,
    day_of_month    TINYINT         NOT NULL,
    day_of_week     TINYINT         NOT NULL,
    day_name        VARCHAR(20)     NOT NULL,
    is_weekend      BIT             NOT NULL
);

CREATE TABLE IF NOT EXISTS gold.dim_service_type (
    service_key     INT             IDENTITY(1,1) PRIMARY KEY,
    carrier_code    VARCHAR(50)     NOT NULL,
    service_code    VARCHAR(200)    NOT NULL,
    service_name    VARCHAR(500)    NULL,
    created_at      DATETIME2       NOT NULL DEFAULT GETUTCDATE()
);

CREATE UNIQUE INDEX uix_dim_service ON gold.dim_service_type (carrier_code, service_code);

-- =============================================================
-- GOLD FACT TABLE
-- =============================================================

CREATE TABLE IF NOT EXISTS gold.fact_invoice (
    invoice_key         BIGINT          IDENTITY(1,1) PRIMARY KEY,
    invoice_number      VARCHAR(100)    NOT NULL,
    invoice_date_key    INT             NULL REFERENCES gold.dim_date(date_key),
    shipment_date_key   INT             NULL REFERENCES gold.dim_date(date_key),
    carrier_key         INT             NULL REFERENCES gold.dim_carrier(carrier_key),
    service_key         INT             NULL REFERENCES gold.dim_service_type(service_key),
    tracking_number     VARCHAR(100)    NULL,
    purchase_contract   VARCHAR(200)    NULL,
    origin_country      VARCHAR(10)     NULL,
    destination_country VARCHAR(10)     NULL,
    weight_kg           FLOAT           NULL,
    charge_amount       DECIMAL(18,4)   NULL,
    charge_currency     VARCHAR(10)     NULL,
    account_number      VARCHAR(100)    NULL,
    _source_name        VARCHAR(200)    NULL,
    _source_file        VARCHAR(500)    NULL,
    _run_id             VARCHAR(500)    NULL,
    loaded_at           DATETIME2       NOT NULL DEFAULT GETUTCDATE()
);

CREATE INDEX ix_fact_invoice_carrier  ON gold.fact_invoice (carrier_key);
CREATE INDEX ix_fact_invoice_inv_date ON gold.fact_invoice (invoice_date_key);
CREATE INDEX ix_fact_invoice_contract ON gold.fact_invoice (purchase_contract);
