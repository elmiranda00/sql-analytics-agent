-- Staging: products
-- Light cleaning only. One row per product.

WITH source AS (
    SELECT * FROM {{ source('thelook_ecommerce', 'products') }}
)

SELECT
    id                              AS product_id,
    name                            AS product_name,
    brand,
    category,
    department,
    CAST(retail_price AS FLOAT64)   AS retail_price,
    CAST(cost AS FLOAT64)           AS product_cost,
    retail_price - cost             AS gross_margin
FROM source
WHERE id IS NOT NULL
