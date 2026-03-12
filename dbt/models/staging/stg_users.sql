-- Staging: users
-- Light cleaning only. One row per customer.

WITH source AS (
    SELECT * FROM {{ source('thelook_ecommerce', 'users') }}
)

SELECT
    id                                              AS user_id,
    first_name,
    last_name,
    email,
    age,
    gender,
    country,
    city,
    DATE(created_at)                                AS signup_date
FROM source
WHERE id IS NOT NULL
