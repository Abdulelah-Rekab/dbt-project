{{
    config(
        materialized = 'incremental',
        unique_key = 'request_id'
    )
}}

SELECT
rq.request_id,
rq.ride_id,
rq.rider_id,
CONCAT(ri.first_name, ' ', ri.last_name) AS rider_name,
ri.gender,
ri.is_active AS rider_is_active,
ri.account_created_date AS rider_account_created_date,
rq.driver_id,
CONCAT(dr.first_name, ' ', dr.last_name) AS driver_name,
dr.is_active AS driver_is_active,
dr.account_created_date AS driver_account_created_date,
rq.completed_trip,
rq.cancelled_trip,
rq.no_show_trip,
rq.seat_unavailable_trip,
rq.driver_unavailable_trip,
rq.unknown_status_trip,
rq.request_creation_time,
rq.passengers_vol,
rq.origin_zone,
rq.destination_zone,
rq.request_status,
rq.ride_price,
rq.actual_pickup_time,
rq.actual_dropoff_time,
rq.geo_city,
rq.day,
rq.week,
rq.month,
rq.year,
p.payment_time,
p.payment_method,
p.payment_status,
p.amount
FROM {{ ref('stg_requests') }} AS rq
LEFT JOIN {{ ref('stg_riders') }} AS ri ON rq.rider_id = ri.rider_id
LEFT JOIN {{ ref('stg_drivers') }} AS dr ON rq.driver_id = dr.driver_id
LEFT JOIN {{ ref('stg_payments') }} AS p ON rq.ride_id = p.ride_id
WHERE rq.request_id IS NOT NULL

{% if is_incremental()%}
    AND rq.request_creation_time >= (SELECT MAX(request_creation_time) FROM {{ this }})
{% endif %}