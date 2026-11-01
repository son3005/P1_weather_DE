import json
import logging

import requests

# initiate logger
logger = logging.getLogger(__name__)

def fetch_weather_data(url: str, timeout: int = 5) -> json:
    """
    Gọi HHTP GET tới URL.
    - Bắt riêng lỗi Timeout -> log WARNING (vì có thể retry).
    - Bắt riêng lỗi HTTPError (4xx) -> log ERROR (không nên retry).
    """
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status() # nếu lỗi >=400 ném ra HTTPError
        return response.json()
    except requests.exceptions.Timeout as e:
        logger.warning(f"Lỗi kết nối Timeout khi gọi {url}. Lỗi này có thể thử lại! Chi tiết: {e}")
        raise
    except requests.exceptions.HTTPError as e:
        if 400 <= response.status_code < 500:
            logger.error(f"Lỗi Client {response.status_code} khi gọi {url}. KHÔNG ĐƯỢC retry! Chi tiết: {e}")
        else:
            logger.warning(f"Lỗi Server {response.status_code}. Có thể retry! Chi tiết: {e}")
        raise
        