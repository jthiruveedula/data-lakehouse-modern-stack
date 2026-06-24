{{ config(
    materialized  = 'table',
    file_format   = 'delta',
    partition_by  = {'field': 'order_date', 'data_type': 'date'},
    cluster_by    = ['region', 'tier'],
) }}

SELECT
    o.order_date,
    c.region,
    c.tier                                          AS customer_tier,
    COUNT(DISTINCT o.order_id)                      AS order_count,
    COUNT(DISTINCT o.customer_id)                   AS unique_customers,
    SUM(o.amount)                                   AS total_revenue,
    AVG(o.amount)                                   AS avg_order_value,
    PERCENTILE_APPROX(o.amount, 0.5)                AS median_order_value,
    PERCENTILE_APPROX(o.amount, 0.95)               AS p95_order_value,
    AVG(o.fulfillment_days)                         AS avg_fulfillment_days,
    SUM(CASE WHEN o.status = 'returned' THEN 1 END) AS returns,
    ROUND(
        SUM(CASE WHEN o.status = 'returned' THEN 1 END) * 100.0
        / NULLIF(COUNT(*), 0), 2
    )                                               AS return_rate_pct
FROM {{ ref('fct_orders') }} o
JOIN {{ ref('dim_customers') }} c
    ON o.customer_id = c.customer_id
    AND c._is_current = TRUE
WHERE o.status != 'cancelled'
GROUP BY 1, 2, 3
