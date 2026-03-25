-- fact_invoice: core fact table
-- Merges Silver carrier_invoice rows into gold.fact_invoice.
-- Deduplicates on (invoice_number, carrier_key) — same logic as the
-- former gold.load_fact_invoice stored procedure.
-- invoice_key (IDENTITY BIGINT) is auto-assigned by SQL Server on insert.
--
-- Execution order (enforced by dbt DAG):
--   silver.carrier_invoice → dim_carrier, dim_service_type, dim_date → fact_invoice

{{
    config(
        unique_key=['invoice_number', 'carrier_key'],
        merge_update_columns=[
            'invoice_date_key', 'shipment_date_key', 'service_key',
            'tracking_number', 'purchase_contract',
            'origin_country', 'destination_country',
            'weight_kg', 'charge_amount', 'charge_currency',
            'account_number', '_source_file', '_run_id', 'loaded_at'
        ]
    )
}}

SELECT
    s.invoice_number,
    TRY_CAST(FORMAT(s.invoice_date,  'yyyyMMdd') AS INT)   AS invoice_date_key,
    TRY_CAST(FORMAT(s.shipment_date, 'yyyyMMdd') AS INT)   AS shipment_date_key,
    dc.carrier_key,
    dst.service_key,
    s.tracking_number,
    s.purchase_contract,
    s.origin_country,
    s.destination_country,
    s.weight_kg,
    s.charge_amount,
    s.charge_currency,
    s.account_number,
    s._source_name,
    s._source_file,
    s._run_id,
    GETUTCDATE()                                           AS loaded_at
FROM {{ source('silver', 'carrier_invoice') }}       AS s
LEFT JOIN {{ ref('dim_carrier') }}          AS dc
    ON  dc.carrier_code = s.carrier
LEFT JOIN {{ ref('dim_service_type') }}     AS dst
    ON  dst.carrier_code  = s.carrier
    AND dst.service_code  = s.service_type
