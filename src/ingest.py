import argparse
import logging
from datetime import UTC, datetime

from src.client import fetch_weather_api
from src.config import CITIES, HOURLY_VARIABLES, OPEN_METEO_ARCHIVE_URL
from src.ops import log_pipeline_run
from src.parser import flatten_weather_json
from src.storage import save_records_to_parquet_minio

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def run_ingestion(start_date: str, end_date: str, custom_run_id: str = None) -> str:
    started_at = datetime.now(UTC)
    dt_str = started_at.strftime("%Y-%m-%d")
    run_id = custom_run_id or started_at.strftime("%Y%m%dT%H%M%S")

    logger.info(f"=== BẮT ĐẦU INGESTION [run_id: {run_id}] từ {start_date} đến {end_date} ===")
    
    all_records = []
    status = "SUCCESS"

    try:
        for city in CITIES:
            logger.info(f"Đang kéo dữ liệu thời tiết cho: {city['city_name']} ({city['city_code']})...")
            
            params = {
                "latitude": city["lat"],
                "longitude": city["lon"],
                "start_date": start_date,
                "end_date": end_date,
                "hourly": ",".join(HOURLY_VARIABLES),
                "timezone": "UTC",
            }

            raw_data = fetch_weather_api(OPEN_METEO_ARCHIVE_URL, params=params)
            city_records = flatten_weather_json(
                raw_json=raw_data,
                city_code=city["city_code"],
                run_id=run_id,
                source_url=OPEN_METEO_ARCHIVE_URL,
            )
            all_records.extend(city_records)
            logger.info(f"-> Thu thập được {len(city_records)} dòng cho {city['city_name']}.")

        # Lưu toàn bộ dữ liệu vào MinIO theo đúng Partition Layout của ADR 0002
        s3_key = f"open_meteo/hourly/dt={dt_str}/run={run_id}/data.parquet"
        rows_saved = save_records_to_parquet_minio(all_records, bucket="bronze", s3_key=s3_key)
        logger.info(f"Đã lưu thành công {rows_saved} dòng vào s3://bronze/{s3_key}")

    except Exception as e:
        status = "FAILED"
        logger.error(f"Pipeline gặp sự cố nghiêm trọng: {e}")
        raise
    finally:
        # Luôn luôn ghi nhận vào ops.run_log dù thành công hay thất bại
        duration_ms = int((datetime.now(UTC) - started_at).total_seconds() * 1000)
        log_pipeline_run(
            run_id=run_id,
            rows_in=len(all_records),
            rows_out=len(all_records) if status == "SUCCESS" else 0,
            started_at=started_at,
            duration_ms=duration_ms,
            status=status,
        )
        logger.info(f"Đã ghi nhận ops.run_log: status={status}, duration={duration_ms}ms, rows={len(all_records)}")

    return run_id

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest weather data into Bronze layer")
    parser.add_argument("--start-date", default="2024-01-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", default="2024-01-07", help="End date (YYYY-MM-DD)")
    parser.add_argument("--run-id", default=None, help="Custom run_id to test idempotency")
    args = parser.parse_args()

    run_ingestion(args.start_date, args.end_date, args.run_id)