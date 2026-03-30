-- dim_service_type: service type dimension
-- Upserts distinct (carrier, service_type) combinations.
-- service_key (IDENTITY) is assigned by SQL Server on insert.

{{
    config(
        unique_key=['carrier_code', 'service_code'],
        merge_update_columns=['service_name']
    )
}}

SELECT DISTINCT
    carrier                  AS carrier_code,
    service_type             AS service_code,
    service_type             AS service_name,
    GETUTCDATE()             AS created_at
FROM {{ ref('carrier_invoice') }}
WHERE service_type IS NOT NULL
