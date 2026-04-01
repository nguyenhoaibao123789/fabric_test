-- dbt: Unified carrier invoice
-- Maps each carrier's staging columns to a canonical schema and UNIONs them.

--
-- To add a new carrier:
--   1. Add its staging table to sources.yml
--   2. Add a CTE below with the column mapping
--   3. Add it to the final UNION ALL

{{
    config(
        unique_key=[
            'invoice_number', 'carrier', 'md_source_file'
        ],
        merge_update_columns=[
            'invoice_date', 'purchase_contract', 'tracking_number',
            'shipment_date', 'origin_country', 'destination_country',
            'weight_kg', 'charge_amount', 'charge_currency',
            'service_type', 'account_number',
            'md_source_name', 'md_run_id'
        ]
    )
}}

WITH fedex AS (
    SELECT
        CAST(invoice_no    AS VARCHAR(8000)) AS invoice_number,
        invoice_date,
        'fedex'                              AS carrier,
        'fedex_direct_invoice'               AS purchase_contract,
        CAST(tracking_id   AS VARCHAR(8000)) AS tracking_number,
        ship_date                            AS shipment_date,
        CAST(NULL          AS VARCHAR(8000)) AS origin_country,
        CAST(NULL          AS VARCHAR(8000)) AS destination_country,
        CAST(weight_lbs AS FLOAT) * 0.453592 AS weight_kg,
        CAST(net_charge    AS VARCHAR(8000)) AS charge_amount,
        CAST(currency      AS VARCHAR(8000)) AS charge_currency,
        CAST(service_code  AS VARCHAR(8000)) AS service_type,
        CAST(account_no    AS VARCHAR(8000)) AS account_number,
        md_source_name,
        md_source_file,
        md_run_id
    FROM {{ source('silver', 'staging_fedex_direct_invoice') }}
    {% if is_incremental() %}
    WHERE md_source_file NOT IN (SELECT DISTINCT md_source_file FROM {{ this }} WHERE carrier = 'fedex')
    {% endif %}
),

dhl AS (
    SELECT
        CAST(invoice_number AS VARCHAR(8000)) AS invoice_number,
        billing_date                          AS invoice_date,
        'dhl'                                 AS carrier,
        'dhl_direct_invoice'                  AS purchase_contract,
        CAST(waybill_no     AS VARCHAR(8000)) AS tracking_number,
        shipment_date,
        CAST(NULL           AS VARCHAR(8000)) AS origin_country,
        CAST(NULL           AS VARCHAR(8000)) AS destination_country,
        CAST(weight_kg      AS FLOAT)         AS weight_kg,
        CAST(total_charge   AS VARCHAR(8000)) AS charge_amount,
        CAST(currency_code  AS VARCHAR(8000)) AS charge_currency,
        CAST(product_code   AS VARCHAR(8000)) AS service_type,
        CAST(shipper_account AS VARCHAR(8000)) AS account_number,
        md_source_name,
        md_source_file,
        md_run_id
    FROM {{ source('silver', 'staging_dhl_direct_invoice') }}
    {% if is_incremental() %}
    WHERE md_source_file NOT IN (SELECT DISTINCT md_source_file FROM {{ this }} WHERE carrier = 'dhl')
    {% endif %}
),

ups AS (
    SELECT
        CAST(invoice_nbr        AS VARCHAR(8000)) AS invoice_number,
        invoice_dt                                 AS invoice_date,
        'ups'                                      AS carrier,
        'ups_carrier_report'                       AS purchase_contract,
        CAST(pkg_tracking_no    AS VARCHAR(8000))  AS tracking_number,
        pickup_date                                AS shipment_date,
        CAST(NULL               AS VARCHAR(8000))  AS origin_country,
        CAST(NULL               AS VARCHAR(8000))  AS destination_country,
        CAST(billed_weight      AS FLOAT)          AS weight_kg,
        CAST(billed_amount      AS VARCHAR(8000))  AS charge_amount,
        CAST(currency           AS VARCHAR(8000))  AS charge_currency,
        CAST(service_description AS VARCHAR(8000)) AS service_type,
        CAST(ups_account        AS VARCHAR(8000))  AS account_number,
        md_source_name,
        md_source_file,
        md_run_id
    FROM {{ source('silver', 'staging_ups_carrier_report') }}
    {% if is_incremental() %}
    WHERE md_source_file NOT IN (SELECT DISTINCT md_source_file FROM {{ this }} WHERE carrier = 'ups')
    {% endif %}
)

SELECT * FROM fedex
UNION ALL
SELECT * FROM dhl
UNION ALL
SELECT * FROM ups
