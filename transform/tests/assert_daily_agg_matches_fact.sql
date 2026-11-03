-- Singular test đối chiếu: Số dòng trong agg_weather_daily phải khớp 100% với số lượng thành phố x ngày trong Fact
WITH fact_days AS (
    SELECT
         count(DISTINCT city_code || '_' || date_id::VARCHAR) as total_fact_city_days
    FROM {{ ref('fct_weather_hourly')}}
),
agg_days AS(
    SELECT count(*) as total_agg_rows
    FROM {{ref('agg_weather_daily')}}
)
-- Trả về dòng lỗi nếu 2 con số này bị lệch nhau
SELECT *
FROM fact_days, agg_days
WHERE total_fact_city_days != total_agg_rows