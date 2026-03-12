-- Staging: orders
-- Light cleaning only. One row per order.

WITH source AS (
    SELECT * FROM {{ source('thelook_ecommerce', 'orders') }}
)

SELECT
    order_id,
    user_id,
    LOWER(status)                                   AS status,
    DATE(created_at)                                AS order_date,
    created_at                                      AS ordered_at,
    returned_at,
    num_of_item,
    -- Derived flags
    (status = 'Returned')                           AS is_returned,
    (status = 'Cancelled')                          AS is_cancelled,
    (status IN ('Complete', 'Shipped'))             AS is_fulfilled
FROM source
WHERE order_id IS NOT NULL
