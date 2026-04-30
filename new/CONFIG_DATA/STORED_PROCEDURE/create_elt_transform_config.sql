/* ============================================================
   CREATE TABLE  mdf_platform_orchestration.elt_transform_config
   Config-driven per-table transformation rules applied by
   bronze2silver_transform_nbk (Cell 4c) before silver load.
   ============================================================ */

CREATE TABLE [mdf_platform_orchestration].[elt_transform_config] (

    [rule_id]         INT           NOT NULL,   -- surrogate key
    [table_id]        INT           NOT NULL,   -- FK → elt_table_config (bronze2silver row)

    -- ── Transform type ─────────────────────────────────────────
    -- add_literal_column : add a constant-value column (e.g. site name for CSAT tables)
    -- remove_column       : drop one or more columns (comma-sep in rule_value)
    -- filter_rows         : keep rows matching a pandas query string in rule_value
    -- dedup               : drop duplicate rows; rule_value = comma-sep subset cols or NULL for all
    -- split_datetime      : split source_column into two cols named in rule_value "date_col,time_col"
    -- union_table         : concat a secondary bronze table; rule_value = its table_id
    -- unpivot             : melt wide → long; rule_value = "id_vars=…;value_vars=…;var_name=…;val_name=…"
    [transform_type]  VARCHAR(50)   NOT NULL,

    -- ── Column references ──────────────────────────────────────
    [source_column]   VARCHAR(200)  NULL,       -- input column (split_datetime, unpivot)
    [target_column]   VARCHAR(200)  NULL,       -- output column name (add_literal_column)

    -- ── Parameterisation ──────────────────────────────────────
    [rule_value]      VARCHAR(500)  NULL,       -- transform-type-specific value (see above)

    -- ── Ordering ──────────────────────────────────────────────
    [sequence_number] INT           NOT NULL    -- execution order within a table_id (lower runs first)
);
