/* ============================================================
   CREATE TABLE  mdf_platform_orchestration.elt_dq_config
   Per-column data quality rules evaluated by
   bronze2silver_dq_nbk AFTER silver load.
   Fabric Warehouse (T-SQL) — idempotent
   ============================================================ */

IF OBJECT_ID(N'[mdf_platform_orchestration].[elt_dq_config]', 'U') IS NULL
BEGIN
    CREATE TABLE [mdf_platform_orchestration].[elt_dq_config] (

        [rule_id]     INT           NOT NULL,   -- surrogate key
        [table_id]    INT           NOT NULL,   -- FK → elt_table_config (bronze2silver row)
        [column_name] VARCHAR(200)  NOT NULL,   -- silver column to validate

        -- ── Rule type ──────────────────────────────────────────────
        -- allowed_values : rule_value = comma-sep list e.g. 'A,B,C'
        -- not_null       : rule_value = NULL (no param needed)
        -- range          : rule_value = 'min:0,max:100'
        -- regex          : rule_value = regex pattern string
        [rule_type]   VARCHAR(50)   NOT NULL,

        [rule_value]  VARCHAR(500)  NULL,       -- rule-type-specific parameter (see above)

        -- ── Failure action ─────────────────────────────────────────
        -- warn   : log the failure count, do not block pipeline
        -- flag   : add a dq_fail_<rule_id> column to the silver table row
        -- reject : remove failing rows before load
        [action]      VARCHAR(20)   NOT NULL
            CONSTRAINT [df_dq_action] DEFAULT ('warn'),

        [is_active]   BIT           NOT NULL
            CONSTRAINT [df_dq_active] DEFAULT (1),

        CONSTRAINT [PK_elt_dq_config] PRIMARY KEY ([rule_id])
    );
END;
GO

/* ============================================================
   CREATE TABLE  mdf_platform_orchestration.elt_dq_results
   Audit log written by bronze2silver_dq_nbk after each run.
   ============================================================ */

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
