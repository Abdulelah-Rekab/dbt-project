WITH ranked_riders AS (
SELECT
CAST(rider_id AS INT64) AS rider_id,
CAST(first_name AS STRING) AS first_name,
CAST(last_name AS STRING) AS last_name,
CAST(phone_number AS STRING) AS phone_number,
CAST(gender AS STRING) AS gender,
CAST(is_active AS BOOLEAN) AS is_active,
CAST(account_created_date AS DATE) AS account_created_date,
CAST(last_update_dt AS TIMESTAMP) AS last_update_dt,
CAST(geo_city AS STRING) AS geo_city,
ROW_NUMBER() OVER (PARTITION BY rider_id ORDER BY last_update_dt DESC) AS row_num
FROM {{ source('via_prod', 'raw_riders_history') }}
WHERE rider_id IS NOT NULL
)

SELECT
*
FROM ranked_riders
WHERE row_num = 1