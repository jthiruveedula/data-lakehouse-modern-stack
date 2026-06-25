{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'merge',
    unique_key           = 'order_id',
    file_format          = 'delta',
    on_schema_change     = 'sync_all_columns',
) }}

WITH src AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY order_id
               ORDER BY     _ingestion_ts DESC
           ) AS rn
    FROM {{ source('bronze', 'orders') }}
    {% if is_incremental() %}
    WHERE _ingestion_ts > (SELECT MAX(_silver_ts) FROM {{ this }})
    {% endif %}
)

SELECT
    order_id,
    customer_id,
    product_id,
    status,
    amount,
    currency,
    order_date,
    shipped_date,
    DATEDIFF(shipped_date, order_date)    AS fulfillment_days,
    current_timestamp()                   AS _silver_ts
FROM src
WHERE rn = 1
  AND order_id IS NOT NULL
  AND amount   >= 0
