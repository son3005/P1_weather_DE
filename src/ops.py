import os
import psycopg2
from datetime import datetime
from typing import Optional

def get_db_connection():
    """Kết nối tới Postgres (metadata database)."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "admin"),
        dbname=os.getenv("POSTGRES_DB", "P1"),
    )

def init_ops_tables():
    """Tạo schema ops và bảng run_log nếu chưa tồn tại."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS ops;")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ops.run_log (
                    run_id VARCHAR(50) PRIMARY KEY,
                    rows_in INT,
                    rows_out INT,
                    started_at TIMESTAMP WITH TIME ZONE,
                    duration_ms BIGINT,
                    status VARCHAR(20)
                );
            """)
            conn.commit()
    finally:
        conn.close()

def log_pipeline_run(
    run_id: str,
    rows_in: int,
    rows_out: int,
    started_at: datetime,
    duration_ms: int,
    status: str,
):
    """Ghi nhận hoặc cập nhật kết quả phiên chạy vào ops.run_log."""
    init_ops_tables()
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ops.run_log (run_id, rows_in, rows_out, started_at, duration_ms, status)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id) DO UPDATE SET
                    rows_in = EXCLUDED.rows_in,
                    rows_out = EXCLUDED.rows_out,
                    duration_ms = EXCLUDED.duration_ms,
                    status = EXCLUDED.status;
            """, (run_id, rows_in, rows_out, started_at, duration_ms, status))
            conn.commit()
    finally:
        conn.close()