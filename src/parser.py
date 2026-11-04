from datetime import UTC, datetime
from typing import Any

from src.config import HOURLY_VARIABLES


def flatten_weather_json(
    raw_json: dict[str, Any],
    city_code: str,
    run_id: str,
    source_url: str,
) -> list[dict[str, Any]]:
    """
    Biến đổi JSON lồng nhau từ Open-Meteo thành danh sách các bản ghi phẳng.
    Bổ sung metadata (_ingested_at, _run_id, _source_url).
    Tự động điền None nếu thiếu biến hoặc mảng dữ liệu bị lệch độ dài.
    """
    hourly = raw_json.get("hourly")
    if not hourly or not isinstance(hourly, dict):
        return []

    times = hourly.get("time")
    if not times or not isinstance(times, list):
        return []

    lat = raw_json.get("latitude")
    lon = raw_json.get("longitude")
    ingested_at = datetime.now(UTC).isoformat()

    records = []
    total_rows = len(times)

    for i in range(total_rows):
        row = {
            "city_code": city_code,
            "latitude": lat,
            "longitude": lon,
            "observed_at": times[i],
            "_ingested_at": ingested_at,
            "_run_id": run_id,
            "_source_url": source_url,
        }

        # Trích xuất từng biến thời tiết đã cấu hình
        for var in HOURLY_VARIABLES:
            var_data = hourly.get(var)
            # Kiểm tra an toàn: nếu biến tồn tại và có giá trị tại vị trí i thì lấy, ngược lại điền None
            if var_data and isinstance(var_data, list) and i < len(var_data):
                row[var] = var_data[i]
            else:
                row[var] = None

        records.append(row)

    return records