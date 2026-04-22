SELECT
rq.driver_id,
rq.geo_city,
CAST(rq.request_creation_time as DATE) AS day,
MIN(CAST(rq.actual_pickup_time AS TIMESTAMP)) AS shift_start_time,
MAX(CAST(rq.actual_dropoff_time AS TIMESTAMP)) AS shift_end_time,
TIMESTAMP_DIFF(MAX(CAST(rq.actual_pickup_time AS TIMESTAMP)), MIN(CAST(rq.actual_dropoff_time AS TIMESTAMP)), MINUTE) AS shift_duration_minutes
FROM {{ ref('stg_requests') }} rq
WHERE rq.driver_id IS NOT NULL
AND rq.request_status = 'Completed'
AND rq.actual_pickup_time IS NOT NULL
AND rq.actual_dropoff_time IS NOT NULL
GROUP BY rq.driver_id, rq.geo_city, day