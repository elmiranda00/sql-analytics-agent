-- Mart: product performance summary
-- One row per product with sales, revenue, and profitability metrics.

WITH product_sales AS (
    SELECT
        product_id,
        product_name,
        brand,
        category,
        department,
        retail_price,
        COUNT(*)                                    AS items_sold,
        COUNT(DISTINCT order_id)                    AS orders,
        COUNT(DISTINCT user_id)                     AS unique_buyers,
        SUM(sale_price)                             AS total_revenue,
        SUM(product_cost)                           AS total_cost,
        SUM(gross_profit)                           AS total_gross_profit,
        SUM(IF(status = 'returned', 1, 0))          AS return_count,
        ROUND(
            SAFE_DIVIDE(
                SUM(IF(status = 'returned', 1, 0)),
                COUNT(*)
            ) * 100, 2
        )                                           AS return_rate_pct
    FROM {{ ref('stg_order_items') }}
    GROUP BY
        product_id, product_name, brand, category, department, retail_price
)

SELECT
    product_id,
    product_name,
    brand,
    category,
    department,
    ROUND(retail_price, 2)                          AS retail_price,
    items_sold,
    orders,
    unique_buyers,
    ROUND(total_revenue, 2)                         AS total_revenue,
    ROUND(total_cost, 2)                            AS total_cost,
    ROUND(total_gross_profit, 2)                    AS total_gross_profit,
    ROUND(SAFE_DIVIDE(total_gross_profit, total_revenue) * 100, 2)
                                                    AS gross_margin_pct,
    return_count,
    return_rate_pct,
    RANK() OVER (ORDER BY total_revenue DESC)       AS revenue_rank
FROM product_sales
ORDER BY total_revenue DESC
