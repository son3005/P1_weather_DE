-- Grain: Mỗi dòng đại diện cho số liệu thời tiết tổng hợp của một thành phố trong một tháng.

SELECT
    city_code,
    year(observed_at_local) as year,
    month(observed_at_local) as month,
    round(avg(temperature_2m),2) as avg_temperature,
    round(min(temperature_2m), 2) AS min_temperature,
    round(max(temperature_2m), 2) AS max_temperature,
    round(avg(relative_humidity_2m), 2) AS avg_humidity,
    round(sum(precipitation), 2) AS total_precipitation,
    round(max(wind_speed_10m), 2) AS max_wind_speed
FROM {{ ref('fct_weather_hourly') }}
GROUP BY city_code, year, month