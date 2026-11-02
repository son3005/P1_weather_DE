--- Trả về các dòng bị lỗi nếu thời gian quan trắc ở tương lai

SELECT
    weather_id,
    city_code,
    observed_at_utc
FROM {{ ref('weather_hourly')}}
WHERE observed_at_utc > CURRENT_TIMESTAMP