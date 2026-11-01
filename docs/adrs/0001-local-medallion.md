# ADR 0001: Chọn DuckDB cho tầng Transform trên môi trường local
- Status: OK
- Context: Pipeline ELT chạy local trên laptop 16 GB RAM DDR4, cần biến đổi dữ liệu medallion từ Bronze (Parquet) sang Silver/Gold.
- Decision: Chọn DuckDB làm engineer transform thay vì PostgreSQL.
- Reason: DuckDB là in-process OLAP database, đọc/ghi định dạng Parquet trực tiếp với tốc độ cột (columnar) vượt trội, ít tiêu tốn RAM và không cần phải duy trì server.
- Outcome: Dễ dàng phát triển và test bằng dbt trên local; logic SQL chuẩn ANSI có thể chuyển sang BigQuery/Snowflake sau này không cần sửa nhiều.