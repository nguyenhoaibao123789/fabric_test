-- dim_carrier: carrier dimension
-- Upserts distinct carrier codes from Silver into gold.dim_carrier.
-- carrier_key (IDENTITY) is assigned by SQL Server on insert; dbt never
-- touches it so the pre-created DDL schema is preserved.

{{
    config(
        unique_key='carrier_code',
        merge_update_columns=['carrier_name', 'is_active', 'updated_at']
    )
}}

SELECT DISTINCT
    carrier                  AS carrier_code,
    carrier                  AS carrier_name,
    1                        AS is_active,
    GETUTCDATE()             AS created_at,
    GETUTCDATE()             AS updated_at
FROM {{ ref('carrier_invoice') }}
WHERE carrier IS NOT NULL
