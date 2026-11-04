import io
import os
from typing import Any

import boto3
import pyarrow as pa
import pyarrow.parquet as pq


def get_s3_client():
    """Tạo client kết nối MinIO thông qua chuẩn AWS S3 SDK."""
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER", "miniouser"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
    )

def ensure_bucket_exists(bucket: str = "bronze") -> None:
    """Tự động tạo bucket nếu chưa tồn tại trên S3/MinIO."""
    s3_client = get_s3_client()
    try:
        s3_client.head_bucket(Bucket=bucket)
    except Exception:
        try:
            s3_client.create_bucket(Bucket=bucket)
        except Exception:
            pass

def save_records_to_parquet_minio(
    records: list[dict[str, Any]],
    bucket: str,
    s3_key: str,
) -> int:
    """
    Chuyển đổi danh sách bản ghi thành Parquet và upload lên MinIO.
    - Idempotent: Ghi đè nếu s3_key đã tồn tại.
    - Trả về số dòng đã ghi.
    """
    if not records:
        return 0

    ensure_bucket_exists(bucket)

    # 1. Chuyển list of dicts thành PyArrow Table
    table = pa.Table.from_pylist(records)

    # 2. Ghi table vào buffer bộ nhớ RAM dưới định dạng Parquet (nén Snappy)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    buffer.seek(0)

    # 3. Đẩy lên MinIO (S3)
    s3_client = get_s3_client()
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=buffer.getvalue(),
    )

    return len(records)
