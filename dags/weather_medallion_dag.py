"""
DAG Orchestration cho Pipeline Thời Tiết Medallion
Luồng thực thi 7 bước:
  1. ingest (PythonOperator): Kéo dữ liệu từ Open-Meteo API -> MinIO (Bronze) [Retries=3]
  2. sensor (PythonOperator): Kiểm tra file Parquet tồn tại trên MinIO Bronze
  3. dbt_silver (BashOperator): Transform dữ liệu thô sang Silver
  4. quality_gate_silver (BashOperator): Kiểm thử chất lượng tầng Silver (Retries=0, Fail-fast)
  5. dbt_gold (BashOperator): Xây dựng mô hình Star Schema tầng Gold
  6. quality_gate_gold (BashOperator): Kiểm thử tính toàn vẹn tầng Gold & reconciliation test
  7. publish (PythonOperator): Xuất bản dữ liệu Gold sang Serving PostgreSQL (Idempotent)
"""

from datetime import datetime, timedelta, timezone
import os
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Cấu hình đường dẫn project để import các module trong src/
PROJECT_DIR = os.getenv("AIRFLOW_PROJECT_DIR", "/opt/airflow/project")
if PROJECT_DIR not in sys.path:
    sys.path.append(PROJECT_DIR)


def task_ingest(**context):
    from src.ingest import run_ingestion
    ds = context["ds"]
    conf = context.get("dag_run").conf or {}
    
    # Cho phép truyền ngày linh hoạt hoặc fallback sang 2024-01-08 nếu ds ngoài dải archive API
    start_date = conf.get("start_date")
    end_date = conf.get("end_date")
    
    if not start_date:
        try:
            run_date = datetime.strptime(ds, "%Y-%m-%d").date()
            if run_date > datetime(2024, 12, 31).date():
                start_date = "2024-01-08"
                end_date = "2024-01-08"
            else:
                start_date = ds
                end_date = ds
        except Exception:
            start_date = "2024-01-08"
            end_date = "2024-01-08"
            
    if not end_date:
        end_date = start_date

    run_id = f"airflow_{ds}"
    print(f"[Airflow Ingest] Kéo dữ liệu từ {start_date} đến {end_date} với run_id={run_id}")
    run_ingestion(start_date=start_date, end_date=end_date, custom_run_id=run_id)


def task_sensor(**context):
    from src.storage import get_s3_client
    ds = context["ds"]
    run_id = f"airflow_{ds}"
    s3_client = get_s3_client()
    
    print(f"[Airflow Sensor] Đang kiểm tra file Parquet Bronze cho run_id={run_id}...")
    response = s3_client.list_objects_v2(Bucket="bronze", Prefix="open_meteo/hourly/")
    contents = response.get("Contents", [])
    
    matched = [obj["Key"] for obj in contents if run_id in obj["Key"]]
    if not matched:
        raise FileNotFoundError(f"Sensor thất bại: Không tìm thấy file Parquet Bronze cho run_id={run_id}!")
    
    print(f"[Airflow Sensor] Xác nhận thành công: Tìm thấy {len(matched)} file Parquet Bronze: {matched[0]}")


def task_publish():
    from src.publish import publish_duckdb_to_postgres
    print("[Airflow Publish] Bắt đầu xuất bản DuckDB Gold sang PostgreSQL...")
    publish_duckdb_to_postgres()
    print("[Airflow Publish] Xuất bản thành công!")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,  # Mặc định KHÔNG retry cho các task test và transform
}

with DAG(
    dag_id="weather_medallion_dag",
    default_args=default_args,
    description="Pipeline thời tiết Medallion 7 bước có Quality Gate",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,  # Bắt buộc = 1 vì DuckDB là single-writer in-process
    tags=["weather", "medallion", "dbt", "gold", "serving"],
) as dag:

    # 1. Ingest (chỉ bước này có Retry do phụ thuộc mạng API bên thứ 3)
    step1_ingest = PythonOperator(
        task_id="ingest",
        python_callable=task_ingest,
        retries=3,
        retry_delay=timedelta(seconds=10),
    )

    # 2. Sensor (Kiểm tra MinIO file tồn tại)
    step2_sensor = PythonOperator(
        task_id="sensor",
        python_callable=task_sensor,
        retries=0,
    )

    # 3. dbt Silver
    step3_dbt_silver = BashOperator(
        task_id="dbt_silver",
        bash_command="cd /opt/airflow/project/transform && dbt run --no-partial-parse --select stg_weather_hourly int_weather_deduped weather_hourly --profiles-dir .",
        retries=0,
    )

    # 4. Quality Gate Silver (Chặn đứng pipeline nếu dữ liệu không đạt chuẩn)
    step4_quality_gate_silver = BashOperator(
        task_id="quality_gate_silver",
        bash_command="cd /opt/airflow/project/transform && dbt test --no-partial-parse --select stg_weather_hourly int_weather_deduped weather_hourly --profiles-dir .",
        retries=0,
    )

    # 5. dbt Gold
    step5_dbt_gold = BashOperator(
        task_id="dbt_gold",
        bash_command="cd /opt/airflow/project/transform && dbt run --no-partial-parse --select dim_city dim_date fct_weather_hourly agg_weather_daily agg_weather_monthly --profiles-dir .",
        retries=0,
    )

    # 6. Quality Gate Gold (Kiểm thử quan hệ khóa ngoại và đối chiếu Fact-Aggregate)
    step6_quality_gate_gold = BashOperator(
        task_id="quality_gate_gold",
        bash_command="cd /opt/airflow/project/transform && dbt test --no-partial-parse --select dim_city dim_date fct_weather_hourly agg_weather_daily agg_weather_monthly assert_daily_agg_matches_fact --profiles-dir .",
        retries=0,
    )

    # 7. Publish sang PostgreSQL Serving Layer
    step7_publish = PythonOperator(
        task_id="publish",
        python_callable=task_publish,
        retries=0,
    )

    # Ranh giới luồng thực thi 7 bước
    (
        step1_ingest
        >> step2_sensor
        >> step3_dbt_silver
        >> step4_quality_gate_silver
        >> step5_dbt_gold
        >> step6_quality_gate_gold
        >> step7_publish
    )

