"""
Module cung cấp cơ chế caching cho các hàm bất đồng bộ (async).

Sử dụng `cachetools.TTLCache` để tạo một bộ nhớ đệm trong bộ nhớ (in-memory cache)
với thời gian sống (Time-To-Live), giúp giảm tải cho database và tăng tốc
độ phản hồi của API bằng cách lưu trữ các kết quả đã tính toán.
"""
from cachetools import TTLCache
from functools import wraps
from typing import Any, Callable

# --- Bộ nhớ cache chia sẻ trong ứng dụng ---
# - maxsize=128: Lưu trữ tối đa 128 kết quả gần nhất.
# - ttl=1800: Mỗi item trong cache sẽ hết hạn sau 1800 giây (30 phút).
service_cache = TTLCache(maxsize=128, ttl=1800)


def async_cache(func: Callable) -> Callable:
    """
    Decorator để cache kết quả của các hàm async trong `DashboardService`.

    Decorator này giải quyết vấn đề service được tạo mới cho mỗi request bằng cách
    tạo ra một cache key duy nhất dựa trên các thuộc tính quan trọng của
    instance service (như khoảng thời gian, cửa hàng) và các tham số của hàm.
    Điều này đảm bảo các request với cùng bộ lọc sẽ nhận lại kết quả từ cache.
    """
    @wraps(func)
    async def wrapper(self, *args, **kwargs) -> Any:
        """
        Hàm bao bọc (wrapper) thực hiện logic kiểm tra và lưu cache.
        """
        # Tạo cache key duy nhất từ tên hàm, các thuộc tính của service,
        # và các tham số truyền vào để đảm bảo tính duy nhất.
        key_parts = (
            func.__name__,
            self.period,
            self.start_date.isoformat(),
            self.end_date.isoformat(),
            self.store,
            args,
            frozenset(kwargs.items())   # Chuyển kwargs thành dạng hashable.
        )
        key = hash(key_parts)           # Dùng hash để key ngắn gọn hơn.

        # Kiểm tra và trả về kết quả từ cache nếu tồn tại.
        if key in service_cache:
            return service_cache[key]

        # Nếu không có trong cache, gọi hàm gốc để lấy dữ liệu mới.
        result = await func(self, *args, **kwargs)

        # Lưu kết quả vào cache trước khi trả về.
        service_cache[key] = result
        return result

    return wrapper
