"""
Cấu hình các thành phố và tham số Ingestion.
"""

CITIES=[
    {"city_code": "HAN",
     "city_name": "Hà Nội",
     "lat":21.0285,
     "lon":105.8542
    },
    {"city_code": "SGN",
     "city_name":"TP. Hồ Chí Minh",
     "lat":10.8231,
     "lon":106.6297
    },
    {"city_code": "DAD",
     "city_name":"Đà Nẵng",
     "lat":16.0544,
     "lon":108.2022
    },
    {"city_code": "VCA",
     "city_name": "Cần Thơ",
     "lat":10.0452,
     "lon":105.7469
    },
    {"city_code": "HPH",
     "city_name":"Hải Phòng",
     "lat":20.8449,
     "lon":106.6881
    }
]

# API Endpoint của Open-Meteo cho dữ liệu lịch sử
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

# Các biến thời tiết theo giờ cần thu thập
HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "weather_code",
]