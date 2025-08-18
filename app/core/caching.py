"""
Module cung cấp cơ chế caching cho các hàm bất đồng bộ (async).

Sử dụng `cachetools.TTLCache` để tạo một bộ nhớ đệm trong bộ nhớ (in-memory cache)
với thời gian sống (Time-To-Live). Cơ chế này giúp giảm tải cho database và tăng
tốc độ phản hồi của API bằng cách lưu trữ các kết quả đã được tính toán, đặc biệt
hiệu quả cho các endpoint có lượng truy cập cao và dữ liệu không thay đổi thường xuyên.
"""
from cachetools import TTLCache
from functools import wraps
from typing import Any, Callable

# Khởi tạo một bộ nhớ cache dùng chung cho toàn bộ ứng dụng.
# - maxsize=128: Lưu trữ tối đa 128 kết quả gần nhất. Khi cache đầy, các
#   item cũ nhất sẽ bị loại bỏ để nhường chỗ cho item mới (LRU - Least
#   Recently Used).
# - ttl=1800: Time-To-Live. Mỗi item trong cache sẽ tự động hết hạn sau
#   1800 giây (30 phút), đảm bảo dữ liệu không quá cũ.
service_cache = TTLCache(maxsize=128, ttl=1800)


def async_cache(func: Callable) -> Callable:
    """
    Decorator để cache kết quả của các phương thức async trong `DashboardService`.

    Decorator này giải quyết vấn đề `DashboardService` được tạo mới cho mỗi request
    bằng cách tạo ra một cache key duy nhất. Key này được tạo dựa trên các thuộc
    tính của instance service (đại diện cho các bộ lọc của người dùng) và các
    tham số của phương thức được gọi.

    Điều này đảm bảo rằng các request với cùng bộ lọc sẽ nhận lại kết quả từ cache
    thay vì phải thực hiện lại các truy vấn tốn kém.
    """
    @wraps(func)
    async def wrapper(self, *args, **kwargs) -> Any:
        """
        Hàm bao bọc (wrapper) thực hiện logic kiểm tra và lưu trữ cache.
        """
        # Tạo cache key từ một tuple chứa các thành phần bất biến (immutable)
        # để đảm bảo tính duy nhất và khả năng băm (hashable).
        key_parts = (
            func.__name__,
            self.period,
            self.start_date.isoformat(),
            self.end_date.isoformat(),
            self.store,
            args,
            # frozenset đảm bảo các item trong kwargs được xử lý
            # không phụ thuộc vào thứ tự.
            frozenset(kwargs.items())
        )
        # Sử dụng hàm hash() để tạo ra một key ngắn gọn, hiệu quả cho việc
        # tra cứu trong dictionary của cache.
        key = hash(key_parts)

        # 1. Kiểm tra cache: Nếu key tồn tại, trả về kết quả ngay lập tức.
        if key in service_cache:
            return service_cache[key]

        # 2. Cache miss: Nếu không có trong cache, gọi hàm gốc để thực thi
        #    logic nghiệp vụ và lấy dữ liệu mới.
        result = await func(self, *args, **kwargs)

        # 3. Lưu vào cache: Lưu kết quả mới vào cache với key đã tạo.
        service_cache[key] = result
        return result

    return wrapper
