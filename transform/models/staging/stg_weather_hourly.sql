WITH source AS (
    SELECT * FROM {{ source("bronze","weather_raw") }}
),
rename_and_cast AS (
    SELECT
        city_code,
        latitude::DOUBLE AS latitude,
        longitude::DOUBLE AS longitude,
        observed_at::TIMESTAMP AS observed_at,
        temperature_2m::DOUBLE AS temperature_2m,
        relative_humidity_2m::DOUBLE AS relative_humidity_2m,
        precipitation::DOUBLE AS precipitation,
        wind_speed_10m::DOUBLE AS wind_speed_10m,
        weather_code::INTEGER AS weather_code,
        _ingested_at::TIMESTAMPTZ AS _ingested_at,
        _run_id,
        _source_url
    FROM source
)
SELECT * from rename_and_cast