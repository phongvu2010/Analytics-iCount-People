from cachetools import TTLCache
from functools import wraps
from typing import Callable, Any

# Tạo một bộ nhớ cache duy nhất cho toàn bộ ứng dụng.
# - maxsize=128: Lưu trữ tối đa 128 kết quả gần nhất.
# - ttl=300: Time-To-Live, mỗi item trong cache sẽ hết hạn sau 300 giây (5 phút).
service_cache = TTLCache(maxsize=128, ttl=300)

def async_cache(func: Callable) -> Callable:
    """
    Một decorator tùy chỉnh để cache kết quả của các hàm async trong service.
    Nó giải quyết vấn đề service được tạo mới mỗi request bằng cách tạo key
    dựa trên các thuộc tính của service thay vì chính object service.
    """
    @wraps(func)
    async def wrapper(self, *args, **kwargs) -> Any:
        # --- TẠO CACHE KEY DUY NHẤT ---
        # Đây là phần quan trọng nhất:
        # Key được tạo từ tên hàm và các thuộc tính quan trọng của `self`
        # (period, dates, store) cùng với các tham số khác của hàm.
        # Điều này đảm bảo mỗi bộ lọc khác nhau sẽ có cache key riêng.
        key = (
            func.__name__,
            self.period,
            self.start_date.isoformat(),
            self.end_date.isoformat(),
            self.store,
            args,
            frozenset(kwargs.items()) # Chuyển kwargs thành dạng hashable
        )

        # --- KIỂM TRA VÀ TRẢ VỀ TỪ CACHE ---
        if key in service_cache:
            # print(f"CACHE HIT for key: {key}") # Bỏ comment để debug
            return service_cache[key]

        # --- THỰC THI HÀM VÀ LƯU VÀO CACHE ---
        # print(f"CACHE MISS for key: {key}") # Bỏ comment để debug
        # Nếu không có trong cache, gọi hàm gốc để lấy dữ liệu
        result = await func(self, *args, **kwargs)

        # Lưu kết quả vào cache trước khi trả về
        service_cache[key] = result
        return result
    return wrapper
