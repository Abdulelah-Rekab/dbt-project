SELECT
CAST(rider_id AS INT64) AS rider_id,
CAST(first_name AS STRING) AS first_name,
CAST(last_name AS STRING) AS last_name,
CAST(phone_number AS STRING) AS phone_number,
COALESCE(CAST(gender AS STRING), 'Unknown') AS gender,
CAST(is_active AS BOOLEAN) AS is_active,
CAST(account_created_date AS DATE) AS account_created_date,
CAST(last_update_dt AS TIMESTAMP) AS last_updated_dt,
CAST(geo_city AS STRING) AS geo_city
FROM {{ source('via_prod','raw_riders') }}
WHERE rider_id IS NOT NULL