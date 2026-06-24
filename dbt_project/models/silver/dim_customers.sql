{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'merge',
    unique_key           = 'customer_id',
    file_format          = 'delta',
    on_schema_change     = 'sync_all_columns',
    merge_update_columns = ['email', 'tier', 'region', '_row_hash', '_valid_from', '_valid_to', '_is_current'],
) }}

WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY     _ingestion_ts DESC
        ) AS rn
    FROM {{ source('bronze', 'customers') }}
    {% if is_incremental() %}
    WHERE _ingestion_ts > (SELECT MAX(_valid_from) FROM {{ this }})
    {% endif %}
),

deduped AS (
    SELECT * FROM ranked WHERE rn = 1
)

SELECT
    customer_id,
    email,
    tier,
    region,
    created_at,
    SHA2(CONCAT_WS('|', COALESCE(email,''), COALESCE(tier,''), COALESCE(region,'')), 256) AS _row_hash,
    current_timestamp() AS _valid_from,
    CAST(NULL AS TIMESTAMP)           AS _valid_to,
    TRUE                              AS _is_current,
    current_timestamp()               AS _silver_ts
FROM deduped
