--- Trả về các dòng bị lỗi nếu độ ẩm nằm ngoài khoảng 0 - 100%

SELECT
    weather_id,
    city_code,
    relative_humidity_2m
FROM {{ ref('weather_hourly')}}
WHERE relative_humidity_2m < 0 OR relative_humidity_2m > 100