-- Grain: Mỗi dòng đại diện cho một lần quan trắc thời tieest của một thành phố trong một giờ cụ thể.
WITH silver_weather AS(
    SELECT * FROM {{ ref('weather_hourly')}}
)
SELECT
    weather_id,
    strftime(observed_at_local,'%Y%m%d')::INTEGER AS date_id,
    city_code,
    observed_at_utc,
    observed_at_local,
    temperature_2m,
    relative_humidity_2m,
    precipitation,
    wind_speed_10m,
    weather_code
FROM silver_weather