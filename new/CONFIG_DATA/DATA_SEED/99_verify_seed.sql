/* ============================================================
   VERIFICATION — confirm seed integrity after running all
   0N_seed_*.sql scripts against the Fabric Warehouse.

   Expected results documented inline. All checks should
   return 0 orphan rows and meet the minimum row counts.
   ============================================================ */


-- ── 1. Row counts per table ────────────────────────────────────────────────────
SELECT 'elt_table_config'    AS tbl, COUNT(*) AS row_count FROM [mdf_platform_orchestration].[elt_table_config]
UNION ALL
SELECT 'elt_schema_config'   AS tbl, COUNT(*) AS row_count FROM [mdf_platform_orchestration].[elt_schema_config]
UNION ALL
SELECT 'elt_transform_config'AS tbl, COUNT(*) AS row_count FROM [mdf_platform_orchestration].[elt_transform_config]
UNION ALL
SELECT 'elt_dq_config'       AS tbl, COUNT(*) AS row_count FROM [mdf_platform_orchestration].[elt_dq_config]
UNION ALL
SELECT 'elt_dq_results'      AS tbl, COUNT(*) AS row_count FROM [mdf_platform_orchestration].[elt_dq_results];
-- Expected minimums:
--   elt_table_config     >= 68  (31 src2brz + 31 b2s + 6 format examples)
--   elt_schema_config    >= 362 (WWI columns, table_ids 1–31)
--   elt_transform_config >= 7   (one per transform_type)
--   elt_dq_config        >= 9   (one per rule_type × action sample)
--   elt_dq_results        = 0   (populated at pipeline runtime only)
GO


-- ── 2. elt_table_config: one row per bronze_file_format discriminator ──────────
SELECT [bronze_file_format], COUNT(*) AS cnt
FROM [mdf_platform_orchestration].[elt_table_config]
GROUP BY [bronze_file_format]
ORDER BY [bronze_file_format];
-- Expected: at least one row each for
--   file_as_is, parquet_none_header, sap_table, sharepoint_list
GO


-- ── 3. elt_table_config: datasubject taxonomy coverage ────────────────────────
SELECT [layer], [datasubject], COUNT(*) AS cnt
FROM [mdf_platform_orchestration].[elt_table_config]
GROUP BY [layer], [datasubject]
ORDER BY [layer], [datasubject];
GO


-- ── 4. Orphan check: elt_schema_config → elt_table_config ─────────────────────
SELECT sc.[table_id], sc.[tableschema], sc.[tablename],
       'ORPHAN: no parent in elt_table_config' AS issue
FROM [mdf_platform_orchestration].[elt_schema_config] sc
WHERE NOT EXISTS (
    SELECT 1
    FROM [mdf_platform_orchestration].[elt_table_config] tc
    WHERE tc.[table_id] = sc.[table_id]
);
-- Expected: 0 rows
GO


-- ── 5. Orphan check: elt_transform_config → elt_table_config ──────────────────
SELECT tx.[rule_id], tx.[table_id],
       'ORPHAN: no parent in elt_table_config' AS issue
FROM [mdf_platform_orchestration].[elt_transform_config] tx
WHERE NOT EXISTS (
    SELECT 1
    FROM [mdf_platform_orchestration].[elt_table_config] tc
    WHERE tc.[table_id] = tx.[table_id]
);
-- Expected: 0 rows
GO


-- ── 6. Orphan check: elt_dq_config → elt_table_config ────────────────────────
SELECT dq.[rule_id], dq.[table_id],
       'ORPHAN: no parent in elt_table_config' AS issue
FROM [mdf_platform_orchestration].[elt_dq_config] dq
WHERE NOT EXISTS (
    SELECT 1
    FROM [mdf_platform_orchestration].[elt_table_config] tc
    WHERE tc.[table_id] = dq.[table_id]
);
-- Expected: 0 rows
GO


-- ── 7. Orphan check: b2s ref_table_id → src2brz row ──────────────────────────
SELECT b2s.[table_id] AS b2s_table_id, b2s.[ref_table_id],
       'ORPHAN: ref_table_id has no matching src2brz row' AS issue
FROM [mdf_platform_orchestration].[elt_table_config] b2s
WHERE b2s.[layer] = 'bronze2silver'
  AND b2s.[ref_table_id] IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM [mdf_platform_orchestration].[elt_table_config] src
      WHERE src.[table_id] = b2s.[ref_table_id]
        AND src.[layer]    = 'src2brz'
  );
-- Expected: 0 rows
GO


-- ── 8. Schema config row count by table (spot-check) ─────────────────────────
SELECT [table_id], [tableschema], [tablename], COUNT(*) AS col_count
FROM [mdf_platform_orchestration].[elt_schema_config]
GROUP BY [table_id], [tableschema], [tablename]
ORDER BY [table_id];
-- Expected notable counts: table 5 (people)=21, table 12 (suppliers)=28,
--   table 16 (customers)=30, table 19 (invoices)=25, table 28 (stockitems)=25
GO


-- ── 9. transform_type coverage ────────────────────────────────────────────────
SELECT [transform_type], COUNT(*) AS cnt
FROM [mdf_platform_orchestration].[elt_transform_config]
GROUP BY [transform_type]
ORDER BY [transform_type];
-- Expected: add_literal_column, dedup, filter_rows, remove_column,
--           split_datetime, union_table, unpivot — each >= 1 row
GO


-- ── 10. DQ rule_type and action coverage ──────────────────────────────────────
SELECT [rule_type], [action], COUNT(*) AS cnt
FROM [mdf_platform_orchestration].[elt_dq_config]
GROUP BY [rule_type], [action]
ORDER BY [rule_type], [action];
-- Expected: allowed_values, not_null, range, regex each with multiple action types
GO
