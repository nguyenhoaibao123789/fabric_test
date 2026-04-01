-- dim_date: date dimension
-- Inserts any date keys not already present (no updates needed).
-- Covers both invoice_date and shipment_date from the current batch.

{{
    config(
        unique_key='date_key',
        merge_update_columns=[]
    )
}}
-- dates are immutable once inserted, so no columns need to be updated on merge

WITH invoice_dates AS (
    SELECT DISTINCT TRY_CAST(invoice_date AS DATE) AS dt
    FROM {{ ref('carrier_invoice') }}
    WHERE invoice_date IS NOT NULL
),
shipment_dates AS (
    SELECT DISTINCT TRY_CAST(shipment_date AS DATE) AS dt
    FROM {{ ref('carrier_invoice') }}
    WHERE shipment_date IS NOT NULL
),
all_dates AS (
    SELECT dt FROM invoice_dates
    UNION
    SELECT dt FROM shipment_dates
)

SELECT
    CAST(FORMAT(dt, 'yyyyMMdd') AS INT)                         AS date_key,
    dt                                                          AS full_date,
    CAST(YEAR(dt)                       AS SMALLINT)            AS year,
    CAST(DATEPART(QUARTER,  dt)         AS TINYINT)             AS quarter,
    CAST(MONTH(dt)                      AS TINYINT)             AS month,
    DATENAME(MONTH,         dt)                                 AS month_name,
    CAST(DATEPART(WEEK,     dt)         AS TINYINT)             AS week_of_year,
    CAST(DAY(dt)                        AS TINYINT)             AS day_of_month,
    CAST(DATEPART(WEEKDAY,  dt)         AS TINYINT)             AS day_of_week,
    DATENAME(WEEKDAY,       dt)                                 AS day_name,
    CASE WHEN DATEPART(WEEKDAY, dt) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend
FROM all_dates
WHERE dt IS NOT NULL
