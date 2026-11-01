from datetime import datetime


def parse_weather_time(time_str:str) ->datetime:
    """
    Chuyển đổi chuỗi thời gian dạng 'YYYY-MM-DDTHH:MM' thành datetime.
    Nếu time_str rỗng hoặc sai định dạng, ném lỗi ValueError với thông báo rõ ràng.
    """
    if not time_str or not isinstance(time_str,str):
            raise ValueError("Chuỗi thời gian không được rỗng.")
    try:
        return datetime.strptime(time_str,"%Y-%m-%dT%H:%M")       
    except ValueError as e:
        raise ValueError(f"Định dạng thời gian không hợp lệ: '{time_str}'. Kỳ vọng dạng 'YYYY-MM-DDTHH:MM'") from e