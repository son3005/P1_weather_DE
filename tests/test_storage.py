from unittest.mock import MagicMock, patch

from src.storage import save_records_to_parquet_minio


def test_save_records_to_parquet_minio_empty():
    assert save_records_to_parquet_minio([], "bronze", "test.parquet") == 0

def test_save_records_to_parquet_minio_success():
    sample_records = [
        {"city_code": "HAN", "temperature_2m": 25.5, "_run_id": "r1"},
        {"city_code": "SGN", "temperature_2m": 32.0, "_run_id": "r1"},
    ]

    with patch("src.storage.get_s3_client") as mock_get_client:
        mock_s3 = MagicMock()
        mock_get_client.return_value = mock_s3

        rows = save_records_to_parquet_minio(sample_records, "bronze", "open_meteo/test.parquet")

        assert rows == 2
        # Khẳng định lệnh put_object lên MinIO đã được gọi với đúng bucket và key
        assert mock_s3.put_object.called
        call_kwargs = mock_s3.put_object.call_args[1]
        assert call_kwargs["Bucket"] == "bronze"
        assert call_kwargs["Key"] == "open_meteo/test.parquet"