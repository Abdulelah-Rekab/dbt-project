SELECT
CAST(driver_id AS INT64) AS driver_id,
CAST(first_name AS STRING) AS first_name,
CAST(last_name AS STRING) AS last_name,
COALESCE(CAST(is_active AS BOOLEAN), FALSE) AS is_active,
CAST(vehicle_id AS INT64) AS vehicle_id,
CAST(account_created_date AS DATE) AS account_created_date,
CAST(geo_city AS STRING) AS geo_city
FROM {{ ref('raw_drivers') }}
WHERE driver_id IS NOT NULL