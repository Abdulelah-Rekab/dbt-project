{% macro driver_summary(time_grain) %}

SELECT
{{ time_grain }} AS {{time_grain}},
COUNT(DISTINCT CASE WHEN rq.completed_trip = 1 THEN rq.driver_id END) AS active_drivers,
SUM(CASE WHEN rq.completed_trip = 1 THEN 1 ELSE 0 END) AS completed_trips,

CASE
  WHEN COUNT(rq.request_id) > 0 THEN
    ROUND(SUM(CASE WHEN rq.completed_trip = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(rq.request_id), 2)
  ELSE
    0
END AS completion_rate,
SUM(CASE WHEN rq.completed_trip = 1 THEN py.amount ELSE 0 END) AS total_earnings,
ROUND(
  SAFE_DIVIDE(
    SUM(CASE WHEN rq.completed_trip = 1 THEN py.amount ELSE 0 END),
    COUNT(DISTINCT CASE WHEN rq.completed_trip = 1 THEN rq.driver_id END)
  ), 2) AS avg_revenue_per_driver
FROM {{ ref('stg_requests') }} rq
LEFT JOIN (
  SELECT ride_id, SUM(amount) AS amount
  FROM {{ ref('stg_payments') }}
  GROUP BY ride_id
) py ON rq.ride_id = py.ride_id
WHERE {{ time_grain }} IS NOT NULL
GROUP BY {{ time_grain }}
ORDER BY {{ time_grain }}

{% endmacro %} 