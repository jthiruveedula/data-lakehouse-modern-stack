-- Fail if any negative revenue exists in Gold
SELECT order_id, amount
FROM {{ ref('fct_orders') }}
WHERE amount < 0
