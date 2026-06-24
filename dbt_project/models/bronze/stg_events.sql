{{ config(
    materialized       = 'incremental',
    incremental_strategy = 'append',
    unique_key         = '_batch_id',
    file_format        = 'delta',
    on_schema_change   = 'sync_all_columns',
) }}

SELECT
    event_id,
    event_type,
    user_id,
    session_id,
    properties,
    server_ts,
    _ingestion_ts,
    _source,
    _batch_id,
    _ingestion_date
FROM {{ source('raw', 'events') }}
{% if is_incremental() %}
WHERE _ingestion_ts > (SELECT MAX(_ingestion_ts) FROM {{ this }})
{% endif %}
