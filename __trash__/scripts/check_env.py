import os

from pathlib import Path

# Lấy thư mục làm việc hiện tại
cwd = Path.cwd()
print(f"Thư mục làm việc hiện tại (CWD): {cwd}")

# Đường dẫn đến file .env mà Pydantic sẽ tìm kiếm
env_file_path = cwd / '.env'
print(f"Đang tìm kiếm file .env tại: {env_file_path}")

# Kiểm tra xem file có thực sự tồn tại ở đó không
if env_file_path.exists():
    print("\n✅ THÀNH CÔNG: File .env đã được tìm thấy!")
    print("Nội dung file:")

    # In ra nội dung để kiểm tra
    with open(env_file_path, 'r') as f:
        print(f.read())
else:
    print("\n❌ THẤT BẠI: Không tìm thấy file .env tại đường dẫn trên.")
    print("Vui lòng kiểm tra lại các mục 1 và 2 ở trên.")
