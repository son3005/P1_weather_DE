WITH staging AS (
    SELECT * FROM {{ ref('stg_weather_hourly') }}
),
ranked AS(
    SELECT *,
    ROW_NUMBER() OVER(
        PARTITION BY city_code, observed_at
        ORDER BY _ingested_at DESC
    ) AS row_num
    FROM staging
),
deduped AS (
    SELECT
        city_code,
        latitude,
        longitude,
        observed_at,
        temperature_2m,
        relative_humidity_2m,
        precipitation,
        wind_speed_10m,
        weather_code,
        _ingested_at,
        _run_id,
        _source_url
    FROM ranked
    WHERE row_num = 1
)
SELECT * FROM deduped
