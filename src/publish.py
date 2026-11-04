import io
import logging
import os

import duckdb

from src.ops import get_db_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

TABLES_TO_PUBLISH = [
    "dim_city",
    "dim_date",
    "agg_weather_daily",
    "agg_weather_monthly",
    "fct_weather_hourly"
]

def map_dtype(col_name: str, dtype) -> str:
    """Ánh xạ kiểu dữ liệu sang PostgreSQL chuẩn cho BI."""
    # weather_id là chuỗi băm MD5 nên bắt buộc phải là VARCHAR / TEXT
    if col_name == "weather_id":
        return "VARCHAR(64)"

    d = str(dtype).lower()
    if col_name == "date_id" or col_name in ("year", "month", "day", "weather_code", "total_hours_recorded") or "int" in d:
        return "BIGINT"
    elif "float" in d or "double" in d or any(k in col_name for k in ("temp", "precip", "humidity", "wind", "lat", "lon")):
        return "DOUBLE PRECISION"
    elif "bool" in d:
        return "BOOLEAN"
    elif "date" in d and "time" not in d:
        return "DATE"
    elif "datetime" in d or "timestamp" in d or "observed_at" in col_name:
        return "TIMESTAMP"
    return "TEXT"

def publish_duckdb_to_postgres():
    duckdb_path = os.getenv("DUCKDB_PATH", "transform/weather.duckdb")
    logger.info(f"Kết nối tới DuckDB: {duckdb_path}...")
    duck_con = duckdb.connect(duckdb_path, read_only=True)

    logger.info("Kết nối tới PostgreSQL (Serving layer)...")
    pg_con = get_db_connection()

    try:
        with pg_con.cursor() as cur:
            # Tạo schema gold trên PostgreSQL
            cur.execute("CREATE SCHEMA IF NOT EXISTS gold;")
            pg_con.commit()

            for table in TABLES_TO_PUBLISH:
                logger.info(f"Đang xuất bản bảng '{table}' sang postgres.gold.{table}...")

                duck_df = duck_con.execute(f"SELECT * FROM {table}").fetch_df()
                csv_buffer = io.StringIO()
                duck_df.to_csv(csv_buffer, index=False)
                csv_buffer.seek(0)

                # Tạo bảng với kiểu dữ liệu số và ngày tháng chuẩn xác cho BI
                columns_desc = []
                for col_name, col_type in zip(duck_df.columns, duck_df.dtypes):
                    pg_type = map_dtype(col_name, col_type)
                    columns_desc.append(f'"{col_name}" {pg_type}')

                cur.execute(f"DROP TABLE IF EXISTS gold.{table};")
                cur.execute(f"CREATE TABLE gold.{table} ({', '.join(columns_desc)});")

                # Sử dụng lệnh COPY thần tốc của PostgreSQL để nạp dữ liệu
                cur.copy_expert(f'COPY gold.{table} FROM STDIN WITH CSV HEADER', csv_buffer)
                pg_con.commit()

                # Kiểm tra số dòng sau khi publish
                cur.execute(f"SELECT count(*) FROM gold.{table};")
                rows = cur.fetchone()[0]
                logger.info(f"-> Xuất bản thành công gold.{table}: {rows:,} dòng.")

        logger.info("=== TẤT CẢ CÁC BẢNG GOLD ĐÃ ĐƯỢC XUẤT BẢN THÀNH CÔNG SANG POSTGRESQL ===")

    finally:
        duck_con.close()
        pg_con.close()

if __name__ == "__main__":
    publish_duckdb_to_postgres()
