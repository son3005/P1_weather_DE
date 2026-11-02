from src.parser import flatten_weather_json
import pytest
# 1. Test trường hợp thiếu biến thời tiết (API không trả về 'precipitation')
def test_flatten_missing_variable():
    raw_data = {
        "latitude": 21.0,
        "longitude": 105.8,
        "hourly": {
            "time": ["2024-01-01T00:00", "2024-01-01T01:00"],
            "temperature_2m": [20.5, 21.0],
            # Cố tình KHÔNG có "precipitation", "wind_speed_10m"...
        }
    }
    records = flatten_weather_json(raw_data, "HAN", "run_1", "http://test")
    assert len(records) == 2
    # Cột thiếu phải tự điền None, không được văng lỗi
    assert records[0]["precipitation"] is None
    assert records[0]["temperature_2m"] == 20.5
    assert records[0]["city_code"] == "HAN"
    assert records[0]["_run_id"] == "run_1"

# 2. Test trường hợp mảng time dài hơn mảng số liệu (lệch độ dài)
def test_flatten_mismatched_length():
    raw_data = {
        "latitude": 10.8,
        "longitude": 106.6,
        "hourly": {
            "time": ["2024-01-01T00:00", "2024-01-01T01:00", "2024-01-01T02:00"],
            "temperature_2m": [30.0],  # Chỉ có 1 giá trị, thiếu 2 giá trị sau
        }
    }
    records = flatten_weather_json(raw_data, "SGN", "run_2", "http://test")
    assert len(records) == 3
    assert records[0]["temperature_2m"] == 30.0
    # Dòng 1 và dòng 2 thiếu số liệu nhiệt độ -> phải tự điền None mà không crash
    assert records[1]["temperature_2m"] is None
    assert records[2]["temperature_2m"] is None