-- Staging: distribution_centers
-- Light cleaning only. One row per distribution center.

WITH source AS (
    SELECT * FROM {{ source('thelook_ecommerce', 'distribution_centers') }}
)

SELECT
    id          AS distribution_center_id,
    name,
    latitude,
    longitude
FROM source
