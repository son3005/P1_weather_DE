{{
    config(
        materialized='incremental',
        unique_key='weather_id'
    )
}}
WITH deduped AS(
    SELECT * FROM {{ ref('int_weather_deduped') }}
),
final AS (
    SELECT
     md5(city_code || '_' || observed_at::VARCHAR) AS weather_id,
     city_code,
     latitude,
     longitude,
     observed_at as observed_at_utc,
     observed_at + INTERVAL 7 HOURS AS observed_at_local,
     temperature_2m,
     relative_humidity_2m,
     precipitation,
     wind_speed_10m,
     weather_code,
     _ingested_at,
     _run_id,
     _source_url
    FROM deduped
    {% if is_incremental() %}
    WHERE _ingested_at > (SELECT coalesce(max(_ingested_at),'1970-01-01'::timestamptz) FROM {{this}})
    {% endif %}
)
SELECT * FROM final
