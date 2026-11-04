from unittest.mock import MagicMock, patch

import pytest
import requests

from src.client import fetch_weather_api


# 1. Test Fail-fast với Mock: Giả lập server trả về 404 -> Phải ném lỗi ngay, không retry
def test_fetch_weather_api_404():
    with patch("requests.get") as mock_get:
        mock_404 = MagicMock()
        mock_404.status_code = 404
        # Giả lập hành vi ném HTTPError khi gọi raise_for_status()
        mock_404.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Not Found")
        mock_get.return_value = mock_404

        with pytest.raises(requests.exceptions.HTTPError):
            fetch_weather_api("https://fake-api.com/404", params={}, max_retries=3)

        # Khẳng định: Gặp lỗi 404 thì CHỈ GỌI 1 LẦN duy nhất rồi fail-fast, không retry
        assert mock_get.call_count == 1

# 2. Test Retry với Mock: Giả lập lần 1 bị lỗi 429, lần 2 thành công 200
def test_fetch_weather_api_retry_on_429():
    with patch("requests.get") as mock_get:
        # Mock lần 1: Server trả về 429 (Rate limit)
        mock_429 = MagicMock()
        mock_429.status_code = 429
        mock_429.text = "Too Many Requests"

        # Mock lần 2: Server trả về 200 (Thành công)
        mock_200 = MagicMock()
        mock_200.status_code = 200
        mock_200.json.return_value = {"hourly": {"temperature_2m": [25.0]}}

        # Gán kịch bản: Lần 1 fail 429, Lần 2 thành công 200
        mock_get.side_effect = [mock_429, mock_200]

        data = fetch_weather_api("https://fake-api.com", params={}, max_retries=3, backoff_factor=0.01)

        assert data == {"hourly": {"temperature_2m": [25.0]}}
        assert mock_get.call_count == 2