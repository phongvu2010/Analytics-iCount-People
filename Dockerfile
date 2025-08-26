# Stage 1: Chọn một "nền móng" nhẹ nhàng nhưng đủ mạnh mẽ
# python:3.10-slim-bookworm là một lựa chọn tốt vì nó nhỏ gọn và bảo mật.
FROM python:3.10-slim-bookworm AS base

# Thiết lập các biến môi trường cần thiết
ENV PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    PYTHONUNBUFFERED=1

# Thiết lập thư mục làm việc bên trong container
WORKDIR /app

# Cài đặt Poetry
RUN pip install poetry==1.8.2


# Stage 2: Cài đặt các thư viện
# Tách riêng bước này để tận dụng cơ chế caching của Docker.
# Docker sẽ không cần cài lại thư viện nếu file toml/lock không thay đổi.
FROM base AS builder
COPY pyproject.toml poetry.lock ./
RUN poetry install --no-root --no-dev


# Stage 3: Xây dựng image cuối cùng
# Copy các thư viện đã cài và mã nguồn vào image
FROM base AS final
COPY --from=builder /usr/local/lib/python3.10/site-packages /usr/local/lib/python3.10/site-packages

# Sao chép entrypoint script vào image
COPY entrypoint.sh .

# Sao chép toàn bộ code của bạn
COPY . .

# Cấp quyền thực thi cho entrypoint script
RUN chmod +x /app/entrypoint.sh

# # Chạy lệnh init-db để đảm bảo các VIEWs trong DuckDB được tạo sẵn
# # Điều này rất quan trọng để ứng dụng có thể khởi chạy mà không bị lỗi.
# RUN python -m cli init-db

# Mở cổng 8000 để bên ngoài có thể giao tiếp với ứng dụng
EXPOSE 8000

# Thiết lập entrypoint để chạy script của chúng ta
ENTRYPOINT ["/app/entrypoint.sh"]

# Lệnh để khởi chạy ứng dụng khi container bắt đầu
# Sử dụng host 0.0.0.0 để Uvicorn lắng nghe các kết nối từ bên ngoài container.
CMD ["python", "-m", "cli", "serve", "--host", "0.0.0.0", "--reload"]
