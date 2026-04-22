-- choose watermark date for incremental load based on the latest performance_date in the target table

{{config(
    materialized='incremental',
    unique_key=['driver_id', 'performance_date']
)}}

SELECT
sh.driver_id,
dr.first_name AS driver_first_name,
dr.last_name AS driver_last_name,
dr.vehicle_id AS driver_vehicle_id,
DATE(rq.request_creation_time) AS performance_date,
COUNT(*) AS total_trips,
SUM(rq.completed_trip) AS completed_trips,
SUM(rq.cancelled_trip) AS cancelled_trips,
SUM(rq.no_show_trip) AS no_show_trips,
SUM(rq.seat_unavailable_trip) AS seat_unavailable_trips,
SUM(rq.driver_unavailable_trip) AS driver_unavailable_trips,
SUM(rq.unknown_status_trip) AS unknown_status_trips,
SUM(py.amount) AS total_earnings,
CASE
  WHEN COUNT(*) > 0 THEN ROUND(SAFE_DIVIDE(SUM(completed_trip), COUNT(*)) * 100, 2)
  ELSE 0
END AS completion_rate
FROM {{ ref('stg_shifts') }} sh
LEFT JOIN {{ ref('stg_drivers') }} dr ON sh.driver_id = dr.driver_id
LEFT JOIN {{ ref('stg_requests') }} rq ON sh.driver_id = rq.driver_id
LEFT JOIN {{ ref('stg_payments') }} py ON rq.ride_id = py.ride_id
WHERE sh.driver_id IS NOT NULL

{% if is_incremental()%}
    AND DATE(rq.request_creation_time) >= (SELECT MAX(performance_date) FROM {{ this }})
{% endif %}

GROUP BY 1,2,3,4,5