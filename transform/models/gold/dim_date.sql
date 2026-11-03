-- Grain: Mỗi dòng đại diện cho môt ngày duy nhất trong lịch (từ 2022 - 2026)
WITH data_spine AS(
    -- Dùng hàm generate_series của DuckDB để sinh dải ngày từ 2022 - 2026
    SELECT
        unnest(
            generate_series(
                '2022-01-01'::DATE,
                '2026-12-31'::DATE,
                INTERVAL 1 DAY
            )
        )::DATE AS date_day
),
final AS(
    SELECT
        --date_id dạng số nguyên YYYYMMDD (ví dụ: 20240101) để làm khoá ngoại
        strftime(date_day,'%Y%m%d')::INTEGER AS date_id,
        date_day,
        year(date_day) AS year,
        quarter(date_day) as quarter,
        month(date_day) AS month,
        strftime(date_day,'%B') AS month_name,
        day(date_day) AS day,
        dayofweek(date_day) AS day_of_week,
        strftime(date_day,'%A') as day_name,
        CASE
            WHEN dayofweek(date_day) IN (0,6) THEN TRUE ELSE FALSE
        END AS is_weekend
    FROM data_spine
)
SELECT * FROM final