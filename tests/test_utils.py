from datetime import datetime

import pytest

from src.utils import parse_weather_time


# 1. Test trường hợp hợp lệ (Happy path) :))
def test_parse_weather_time_valid():
    result = parse_weather_time("2024-01-01T00:00")
    assert result == datetime(2024,1,1,0,0)

# 2. Test trường hợp chuỗi rỗng
def test_parse_weather_time_emtpy():
    with pytest.raises(ValueError, match="không được rỗng"):
        parse_weather_time("") 

# 3. Test trường hợp chuỗi sai định dạng
def test_parse_weather_time_invalid_format():
    with pytest.raises(ValueError, match="không hợp lệ"):
        parse_weather_time("2024/01/01 00:00")