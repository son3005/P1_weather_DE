import logging
import time
from typing import Any, Dict
import requests

logger = logging.getLogger(__name__)

def fetch_weather_api(
    url: str,
    params: Dict[str, Any],
    max_retries: int = 3,
    backoff_factor: float = 1.0,
    timeout: int = 10,
) -> Dict[str, Any]:
    """
    Gọi Open-Meteo API có cơ chế Retry & Exponential Backoff.
    - Retry khi gặp: Timeout, ConnectionError, HTTP 429, HTTP 5xx.
    - Fail-fast khi gặp: HTTP 4xx khác (400, 404...).
    """
    attempt = 0
    while attempt < max_retries:
        try:
            logger.info(f"Đang gọi API {url} (Lần thử {attempt + 1}/{max_retries})...")
            response = requests.get(url, params=params, timeout=timeout)

            # Nếu gặp 429 hoặc 5xx -> coi như lỗi có thể retry
            if response.status_code == 429 or response.status_code >= 500:
                raise requests.exceptions.RequestException(
                    f"Server trả về mã {response.status_code}: {response.text}"
                )

            response.raise_for_status()
            return response.json()

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError, requests.exceptions.RequestException) as e:
            if isinstance(e, requests.exceptions.HTTPError):
                if response.status_code < 500 and response.status_code != 429:
                    logger.error(f"Lỗi Client {response.status_code}, dừng ngay: {e}")
                    raise

            attempt += 1
            if attempt >= max_retries:
                logger.error(f"Đã thử lại tối đa {max_retries} lần nhưng vẫn thất bại: {e}")
                raise

            sleep_time = backoff_factor * (2 ** (attempt - 1))
            logger.warning(f"Gặp sự cố ({e}). Đang chờ {sleep_time}s trước khi thử lại lần {attempt + 1}...")
            time.sleep(sleep_time)

    # Đặt thẳng hàng với while (4 khoảng trắng), chỉ chạy tới đây khi thoát vòng lặp
    raise RuntimeError("Vượt quá số lần retry mà không có kết quả")