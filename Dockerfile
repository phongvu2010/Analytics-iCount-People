# Stage 1: Build - Cài đặt dependencies với Poetry
FROM python:3.12-slim AS builder

# Cài đặt các gói hệ thống cần thiết cho việc build một số thư viện Python
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Cài đặt Poetry
ENV POETRY_HOME="/opt/poetry"
ENV POETRY_VERSION=1.8.2
RUN python3 -m venv $POETRY_HOME && \
    $POETRY_HOME/bin/pip install --upgrade pip && \
    $POETRY_HOME/bin/pip install "poetry==$POETRY_VERSION"
ENV PATH="$POETRY_HOME/bin:$PATH"

# Sao chép các tệp quản lý dependency
WORKDIR /app
COPY poetry.lock pyproject.toml ./

# === THAY ĐỔI QUAN TRỌNG Ở ĐÂY ===
# Cấu hình Poetry để tạo virtualenv bên trong thư mục dự án
RUN poetry config virtualenvs.in-project true

# Cài đặt các thư viện vào một virtual environment, không bao gồm các gói dev
RUN poetry install --no-root --no-dev


# Stage 2: Final - Tạo image cuối cùng, nhẹ hơn
FROM python:3.12-slim AS final

# Cài đặt các gói ODBC cần thiết để kết nối tới MS SQL Server
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    gnupg \
    unixodbc \
    unixodbc-dev \
    && mkdir -p /etc/apt/keyrings \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /etc/apt/keyrings/microsoft.gpg \
    && echo "deb [arch=arm64,armhf,amd64 signed-by=/etc/apt/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

# Tạo người dùng không phải root để tăng cường bảo mật
ENV APP_USER=appuser
RUN groupadd -r $APP_USER && useradd -r -g $APP_USER $APP_USER

WORKDIR /app

# Sao chép virtual environment đã cài đặt từ stage builder
COPY --from=builder /app/.venv .venv
ENV PATH="/app/.venv/bin:$PATH"

# Sao chép toàn bộ mã nguồn ứng dụng
COPY . .

# Phân quyền cho người dùng mới
RUN chown -R $APP_USER:$APP_USER /app
USER $APP_USER

# Expose port mà FastAPI sẽ chạy
EXPOSE 8000

# Command mặc định để khởi chạy web server
CMD ["poetry", "run", "python", "cli.py", "serve", "--host", "0.0.0.0", "--port", "8000", "--no-reload"]
