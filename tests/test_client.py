import pytest
import requests

from src.client import fetch_weather_data


def test_fetch_weather_data_404():
    # Gọi tới endpoint không tồn tại
    with pytest.raises(requests.exceptions.HTTPError):
        fetch_weather_data("https://httpbin.org/status/404")