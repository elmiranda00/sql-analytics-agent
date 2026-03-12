-- Mart: daily revenue summary
-- Aggregates order-item revenue by day, with 7-day rolling average and DoD growth.

WITH daily AS (
    SELECT
        order_date,
        COUNT(DISTINCT order_id)                    AS order_count,
        COUNT(DISTINCT user_id)                     AS unique_customers,
        SUM(sale_price)                             AS gross_revenue,
        SUM(IF(status = 'returned', sale_price, 0)) AS returned_revenue,
        SUM(sale_price) - SUM(IF(status = 'returned', sale_price, 0))
                                                    AS net_revenue,
        SUM(gross_profit)                           AS total_gross_profit
    FROM {{ ref('stg_order_items') }}
    GROUP BY order_date
)

SELECT
    order_date,
    order_count,
    unique_customers,
    ROUND(gross_revenue, 2)                         AS gross_revenue,
    ROUND(returned_revenue, 2)                      AS returned_revenue,
    ROUND(net_revenue, 2)                           AS net_revenue,
    ROUND(total_gross_profit, 2)                    AS total_gross_profit,
    -- 7-day rolling average of net revenue
    ROUND(
        AVG(net_revenue) OVER (
            ORDER BY order_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2
    )                                               AS net_revenue_7d_avg,
    -- Day-over-day growth (%)
    ROUND(
        SAFE_DIVIDE(
            net_revenue - LAG(net_revenue) OVER (ORDER BY order_date),
            LAG(net_revenue) OVER (ORDER BY order_date)
        ) * 100, 2
    )                                               AS net_revenue_dod_pct
FROM daily
ORDER BY order_date
