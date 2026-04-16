SELECT
CAST(request_id AS INT) AS request_id,
CAST(rider_id AS INT) AS rider_id,
CAST(ride_id AS INT) AS ride_id,
CAST(driver_id AS INT) AS driver_id,
CASE
  WHEN request_status = 'Completed' AND actual_dropoff_time IS NOT NULL THEN 1
  ELSE 0
END as completed_trip,
CASE
  WHEN request_status = 'Cancelled' THEN 1
  ELSE 0
END as cancelled_trip,
CASE
  WHEN request_status = 'No-Showed' THEN 1
  ELSE 0
END as no_show_trip,
CASE
  WHEN request_status = 'Seat Unavailable' THEN 1
  ELSE 0
END as seat_unavailable_trip,
CASE
  WHEN request_status = 'Not Accepted' THEN 1
  ELSE 0
END as driver_unavailable_trip,
CASE
  WHEN request_status = 'Unknown' THEN 1
  ELSE 0
END as unknown_status_trip,
CAST(request_creation_time AS TIMESTAMP) AS request_creation_time,
passengers_vol,
COALESCE(origin_zone, 'Unknown') AS origin_zone,
COALESCE(destination_zone, 'Unknown') AS destination_zone,
COALESCE(request_status, 'Unknown') AS request_status,
ride_price,
CAST(actual_pickup_time AS TIMESTAMP) AS actual_pickup_time,
CAST(actual_dropoff_time AS TIMESTAMP) AS actual_dropoff_time,
COALESCE(geo_city, 'Unknown') AS geo_city,
DATE(request_creation_time) AS day,
DATE_TRUNC(CAST(request_creation_time AS TIMESTAMP), WEEK) AS week,
DATE_TRUNC(CAST(request_creation_time AS TIMESTAMP), MONTH) AS month,
DATE_TRUNC(CAST(request_creation_time AS TIMESTAMP), YEAR) AS year
FROM {{ ref('raw_requests') }}
WHERE request_id IS NOT NULL