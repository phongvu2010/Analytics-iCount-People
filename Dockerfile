# --- Giai đoạn 1: Builder ---
# Giai đoạn này dùng để cài đặt dependencies, tạo ra một môi trường ảo hoàn chỉnh.
FROM python:3.12-slim-bookworm AS builder

# Cài đặt các gói hệ thống cần thiết và Poetry
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
RUN curl -sSL https://install.python-poetry.org | python3 -

# Thêm Poetry vào PATH
ENV PATH="/root/.local/bin:${PATH}"

WORKDIR /app

# Chỉ copy file quản lý dependency để tận dụng cache của Docker
COPY poetry.lock pyproject.toml ./

# Cài đặt dependencies vào một môi trường ảo riêng, không bao gồm các gói dev
RUN poetry config virtualenvs.in-project true && \
poetry install --no-interaction --no-ansi --without dev --no-root

# Copy toàn bộ mã nguồn ứng dụng
COPY . .


# --- Giai đoạn 2: Production Image ---
# Giai đoạn này tạo ra image cuối cùng, chỉ chứa những gì cần thiết để chạy ứng dụng.
FROM python:3.12-slim-bookworm

# Cài đặt các gói cần thiết cho runtime (ví dụ: driver ODBC cho SQL Server)
# Bước 1: Cài đặt tất cả các gói phụ thuộc cần thiết cho driver
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gnupg \
    debconf-utils \
    unixodbc \
    unixodbc-dev \
    odbcinst \
    && rm -rf /var/lib/apt/lists/*

# Bước 2: Tải về gói msodbcsql18 .deb
RUN ARCH=$(dpkg --print-architecture) && \
    curl -fsSL -o /tmp/msodbcsql18.deb "https://packages.microsoft.com/debian/12/prod/pool/main/m/msodbcsql18/msodbcsql18_18.3.3.1-1_${ARCH}.deb"

# Bước 3: Tự động chấp nhận EULA và cài đặt driver (bây giờ sẽ thành công vì dependencies đã có sẵn)
RUN echo "msodbcsql18 msodbcsql/ACCEPT_EULA boolean true" | debconf-set-selections \
    && dpkg -i /tmp/msodbcsql18.deb \
    && rm -f /tmp/msodbcsql18.deb

# Copy file cấu hình OpenSSL tùy chỉnh vào image
COPY openssl.cnf /etc/ssl/openssl.cnf

# Tạo một user không phải root để chạy ứng dụng
RUN useradd --create-home --shell /bin/bash appuser
USER appuser
WORKDIR /home/appuser

# Copy môi trường ảo đã được cài đặt từ giai đoạn 'builder'
COPY --from=builder --chown=appuser:appuser /app/.venv ./.venv

# Copy mã nguồn ứng dụng từ giai đoạn 'builder'
COPY --from=builder --chown=appuser:appuser /app .

# Mở port 8000 để ứng dụng FastAPI có thể nhận request
EXPOSE 8000

# Lệnh mặc định khi container khởi chạy
CMD ["/home/appuser/.venv/bin/python", "cli.py", "serve", "--host", "0.0.0.0"]
