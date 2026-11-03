-- Grain: Mỗi dòng đại diện cho một thành phố duy nhất cùng thông tin địa lý và vùng miền.

WITH source AS (
    SELECT * FROM {{ref('cities')}}
)
SELECT
    city_code,
    city_name,
    region,
    latitude::DOUBLE AS latitude,
    longitude::DOUBLE AS longitude
FROM source