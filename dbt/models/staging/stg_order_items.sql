-- Staging: order_items joined to products
-- One row per order-item, enriched with product details.

WITH order_items AS (
    SELECT * FROM {{ source('thelook_ecommerce', 'order_items') }}
),

products AS (
    SELECT * FROM {{ source('thelook_ecommerce', 'products') }}
)

SELECT
    oi.id                                           AS order_item_id,
    oi.order_id,
    oi.user_id,
    oi.product_id,
    LOWER(oi.status)                                AS status,
    ROUND(oi.sale_price, 2)                         AS sale_price,
    ROUND(p.cost, 2)                                AS product_cost,
    ROUND(oi.sale_price - p.cost, 2)               AS gross_profit,
    DATE(oi.created_at)                             AS order_date,
    oi.created_at                                   AS ordered_at,
    -- Product details
    p.name                                          AS product_name,
    p.brand,
    p.category,
    p.department,
    ROUND(p.retail_price, 2)                        AS retail_price
FROM order_items AS oi
LEFT JOIN products AS p ON oi.product_id = p.id
WHERE oi.id IS NOT NULL
