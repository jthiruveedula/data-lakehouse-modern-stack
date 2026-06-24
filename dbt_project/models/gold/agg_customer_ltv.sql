{{ config(
    materialized = 'table',
    file_format  = 'delta',
    cluster_by   = ['tier', 'region'],
) }}

WITH order_stats AS (
    SELECT
        customer_id,
        COUNT(DISTINCT order_id)                   AS lifetime_orders,
        SUM(amount)                                AS lifetime_revenue,
        AVG(amount)                                AS avg_order_value,
        MIN(order_date)                            AS first_order_date,
        MAX(order_date)                            AS last_order_date,
        DATEDIFF(MAX(order_date), MIN(order_date)) AS customer_tenure_days
    FROM {{ ref('fct_orders') }}
    WHERE status NOT IN ('cancelled', 'returned')
    GROUP BY customer_id
)

SELECT
    c.customer_id,
    c.email,
    c.tier,
    c.region,
    os.lifetime_orders,
    ROUND(os.lifetime_revenue, 2)                                AS lifetime_revenue,
    ROUND(os.avg_order_value, 2)                                 AS avg_order_value,
    os.first_order_date,
    os.last_order_date,
    os.customer_tenure_days,
    CASE
        WHEN os.lifetime_revenue > 10000 THEN 'Champion'
        WHEN os.lifetime_revenue > 5000  THEN 'Loyal'
        WHEN os.lifetime_revenue > 1000  THEN 'Promising'
        ELSE 'At Risk'
    END                                                           AS ltv_segment,
    DATEDIFF(CURRENT_DATE, os.last_order_date)                    AS days_since_last_order
FROM {{ ref('dim_customers') }} c
JOIN order_stats os USING (customer_id)
WHERE c._is_current = TRUE
