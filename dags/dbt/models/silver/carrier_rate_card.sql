-- dbt: Unified carrier rate card
-- Maps each carrier's staging rate card to a canonical schema and UNIONs them.
--
-- To add a new carrier:
--   1. Add its staging table to sources.yml
--   2. Add a CTE below with the column mapping
--   3. Add it to the final UNION ALL

{{
    config(
        unique_key=['carrier', 'effective_date', 'expiry_date', 'md_source_file'],
        merge_update_columns=[
            'base_rate', 'fuel_surcharge_pct', 'md_source_name', 'md_run_id'
        ]
    )
}}

WITH fedex AS (
    SELECT
        'fedex'                AS carrier,
        effective_date,
        expiry_date,
        CAST(base_rate         AS FLOAT) AS base_rate,
        CAST(fuel_surcharge_pct AS FLOAT) AS fuel_surcharge_pct,
        md_source_name,
        md_source_file,
        md_run_id
    FROM {{ source('silver', 'staging_fedex_rate_card') }}
    {% if is_incremental() %}
    WHERE md_source_file NOT IN (SELECT DISTINCT md_source_file FROM {{ this }} WHERE carrier = 'fedex')
    {% endif %}
)

SELECT * FROM fedex
