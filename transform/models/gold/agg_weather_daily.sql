-- Grain: Mỗi dòng đại diện cho số liệu thời tiết tổng hợp của một thành phố trong một ngày.
SELECT
    city_code,
    date_id,
    observed_at_local::DATE AS weather_date,
    count(*) AS total_hours_recorded,
    round(avg(temperature_2m),2) as avg_temperature,
    round(min(temperature_2m),2) as min_temperature,
    round(max(temperature_2m),2) as max_temperature,
    round(avg(relative_humidity_2m),2) as avg_humidity,
    round(sum(precipitation),2) as total_precipitation,
    round(max(wind_speed_10m),2) as max_wind_speed
FROM {{ ref('fct_weather_hourly')}}
GROUP BY city_code,date_id,weather_date