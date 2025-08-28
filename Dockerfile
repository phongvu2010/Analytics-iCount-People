# --- Giai đoạn 1: Builder ---
# Giai đoạn này dùng để cài đặt dependencies, tạo ra một môi trường ảo hoàn chỉnh.
FROM python:3.12-slim as builder

# Cài đặt các gói hệ thống cần thiết và Poetry
RUN apt-get update && apt-get install -y --no-install-recommends \
curl && rm -rf /var/lib/apt/lists/*
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
FROM python:3.12-slim

# Cài đặt các gói cần thiết cho runtime (ví dụ: driver ODBC cho SQL Server)
# Tham khảo: https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server
RUN apt-get update && apt-get install -y --no-install-recommends curl gnupg \
&& curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
&& curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list \
&& apt-get update \
&& ACCEPT_EULA=Y apt-get install -y msodbcsql17 \
&& apt-get clean \
&& rm -rf /var/lib/apt/lists/*

# Tạo một user không phải root để chạy ứng dụng -> Tăng cường bảo mật
RUN useradd --create-home --shell /bin/bash appuser
USER appuser
WORKDIR /home/appuser

# Copy môi trường ảo đã được cài đặt từ giai đoạn 'builder'
COPY --from=builder --chown=appuser:appuser /app/.venv ./.venv

# Copy mã nguồn ứng dụng từ giai đoạn 'builder'
COPY --from=builder --chown=appuser:appuser /app .

# # Thêm môi trường ảo vào PATH để có thể chạy `poetry run`
# ENV PATH="/home/appuser/.venv/bin:${PATH}"

# Mở port 8000 để ứng dụng FastAPI có thể nhận request
EXPOSE 8000

# Lệnh mặc định khi container khởi chạy
# CMD ["poetry", "run", "python", "cli.py", "serve", "--host", "0.0.0.0"]
CMD ["/home/appuser/.venv/bin/python", "cli.py", "serve", "--host", "0.0.0.0"]
