SELECT
day,
COUNT(DISTINCT request_id) AS total_requests,
SUM(passengers_vol) AS total_passengers,
COUNT(DISTINCT CASE WHEN completed_trip = 1 THEN request_id END) AS completed_trips,
SUM(CASE WHEN completed_trip = 1 THEN passengers_vol ELSE 0 END) AS completed_rides,
COUNT(DISTINCT CASE WHEN cancelled_trip = 1 THEN request_id END) AS cancelled_trips,
COUNT(DISTINCT CASE WHEN no_show_trip = 1 THEN request_id END) AS no_show_trips,
COUNT(DISTINCT CASE WHEN seat_unavailable_trip = 1 THEN request_id END) AS seat_unavailable_trips,
COUNT(DISTINCT CASE WHEN driver_unavailable_trip = 1 THEN request_id END) AS driver_unavailable_trips,
COUNT(DISTINCT CASE WHEN unknown_status_trip = 1 THEN request_id END) AS unknown_status_trips,
CASE
  WHEN COUNT(DISTINCT request_id) > 0 THEN
    ROUND(COUNT(DISTINCT CASE WHEN completed_trip = 1 THEN request_id END) * 100.0 / COUNT(DISTINCT request_id), 2)
  ELSE
    0
END AS completion_rate,
SUM(CASE WHEN completed_trip = 1 THEN amount ELSE 0 END) AS total_revenue,
ROUND(AVG(CASE WHEN completed_trip = 1 THEN amount END), 2) AS avg_revenue_per_completed_trip,
ROUND(AVG(CASE WHEN completed_trip = 1 THEN passengers_vol END), 2) AS avg_passengers_per_completed_trip,
ROUND(AVG(CASE WHEN completed_trip = 1 THEN ride_price END), 2) AS avg_ride_price,
ROUND(AVG(CASE WHEN completed_trip = 1 THEN TIMESTAMP_DIFF(actual_dropoff_time, actual_pickup_time, MINUTE) END), 2) AS avg_trip_duration_minutes
FROM {{ ref('Requests') }}
WHERE day IS NOT NULL
GROUP BY day