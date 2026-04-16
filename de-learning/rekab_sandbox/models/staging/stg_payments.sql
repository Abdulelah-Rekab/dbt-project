SELECT
CAST(ride_id AS INT64) AS ride_id,
CAST(rider_id AS INT64) AS rider_id,
CAST(driver_id AS INT64) AS driver_id,
CAST(payment_time AS TIMESTAMP) AS payment_time,
COALESCE(payment_method, 'Unknown') AS payment_method,
COALESCE(payment_status, 'Unknown') AS payment_status,
CAST(amount AS FLOAT64) AS amount,
COALESCE(geo_city, 'Unknown') AS geo_city
FROM {{ ref('raw_payments') }}
WHERE ride_id IS NOT NULL