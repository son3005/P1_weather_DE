# ADR 0002: Thiết kế Metadata, Layout Data Lake và Cửa sổ Ingest cho Bronze Layer
- Status: OK
- Context: Cần kéo dữ liệu thời tiết nhiều năm của 5 thành phố từ Open-Meteo APi và lưu trữ thô (Bronze) trên MinIO dưới dạng file Parquet.
- Decision:
    1. MetadataL Mọi bản ghi bổ sung trường 3 trường: _ingested_at (thời gian nạp UTC), _run_id (mã lần chạy), _source_url (URL nguồn).
    2. Layout Partition: Lưu trữ theo đường dẫn bronze/open-meteo/hourly/dt=<YYYY-MM-DD>/run=<YYYYMMDDTHHMMSS>/data.parquet. Chạy lại cùng một run_id thì ghi đè file để đảm bảo tính Idempotent.
    3. Ingest Window: Backfill dữ liệu 3 năm (2022-2024) cho thành 5 thành phố, chia nhỏ request theo từng khoảng ngày để tránh timeout.
- Outcome: Dễ dàng kiểm toán dữ liệu, truy vết nguồn gốc khi có lỗi, và đảm bảo DuckDB có thể đọc phân vùng hiệu quả ở tầng Silver.