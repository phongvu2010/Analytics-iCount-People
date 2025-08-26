# Analytics iCount People
Hệ thống phân tích và trực quan hóa lưu lượng người ra vào, được xây dựng với kiến trúc hiện đại sử dụng Python, FastAPI, DuckDB và Tailwind CSS.


## Các Công Nghệ Chính
* **Backend:** Python, FastAPI
* **Database:** MS SQL Server (Nguồn), DuckDB (Kho dữ liệu phân tích)
* **ETL Pipeline:** Pandas, Pandera, PyArrow
* **Frontend:** Tailwind CSS, ApexCharts, Litepicker
* **CLI Tool:** Typer


## Sơ đồ cấu trúc dự án
Dự án được tổ chức theo cấu trúc module hóa, tách biệt rõ ràng các mối quan tâm (API, ETL, Core), giúp dễ dàng bảo trì và mở rộng.
```bash
iCount/
├── app/                                # Chứa toàn bộ mã nguồn ứng dụng FastAPI
│   ├── core/                           # Các module lõi (config, caching)
│   │   ├── caching.py
│   │   ├── config.py
│   ├── etl/                            # Logic của pipeline ETL (Extract, Transform, Load)
│   │   ├── __init__.py
│   │   ├── extract.py
│   │   ├── load.py
│   │   ├── schemas.py
│   │   ├── state.py
│   │   └── transform.py
│   ├── utils/                          # Các module tiện ích (logger)
│   │   └── logger.py
│   ├── dependencies.py                 # Quản lý dependency injection
│   ├── main.py                         # Điểm khởi đầu của ứng dụng
│   ├── routers.py                      # Định nghĩa các API endpoints
│   ├── schemas.py                      # Pydantic models cho API
│   └── services.py                     # Chứa logic nghiệp vụ chính
├── configs/                            # Chứa các tệp cấu hình YAML
│   ├── logger.yaml
│   ├── tables.yaml
│   └── time_offsets.yaml
├── data/                               # Nơi lưu trữ tệp DuckDB và trạng thái ETL
├── logs/                               # Nơi lưu trữ tệp log
├── node_modules/
├── template/                           # Chứa các tệp HTML, CSS, JS cho frontend
│   ├── partials/
│   │   ├── _charts.html                # Phần chứa các biểu đồ
│   │   ├── _error_modal.html           # Phần chứa các error modal
│   │   ├── _filters.html               # Phần chứa các bộ lọc
│   │   ├── _footer.html                # Phần footer của trang
│   │   ├── _header.html                # Phần header của trang
│   │   ├── _metrics.html               # Phần chứa các thẻ chỉ số
│   │   ├── _sidebar.html               # Phần chứa các sidebar
│   │   ├── _skeleton.html
│   │   └── _table.html                 # Phần bảng dữ liệu chi tiết
│   ├── statics/
│   │   ├── css/
│   │   │   ├── input.html
│   │   │   └── style.css
│   │   ├── images/
│   │   │   ├── favicon.ico
│   │   │   └── logo.png
│   │   └── js/
│   │       └── dashboard.js            # Toàn bộ code JavaScript
│   ├── base.html                       # File layout chính, chứa cấu trúc chung
│   └── dashboard.html                  # File nội dung chính, kế thừa từ base.html
├── tests/                              # Thư mục chứa các bài test
│   └── __init__.py
├── .env
├── .env.example                        # Tệp môi trường mẫu
├── .gitignore
├── cli.py                              # Giao diện dòng lệnh (CLI) để vận hành
├── package-lock.json
├── package.json
├── pyproject.toml                      # Thay cho requirements.txt để quản lý dependency tốt hơn
├── README.md
└── tailwind.config.js
```

## Hướng Dẫn Cài Đặt và Vận Hành
### 1. Chuẩn Bị Môi Trường
* Cài đặt Python 3.10+ và Node.js.
* Tạo môi trường ảo và cài đặt các dependency của Python:
    ```bash
    python -m venv .venv
    source .venv/bin/activate       # Trên Windows: .venv\Scripts\activate
    pip install -r requirements.txt # Giả sử bạn có file này, hoặc từ pyproject.toml
    ```
* Cài đặt các dependency của Node.js:
    ```bash
    npm install
    ```
* Sao chép `.env.example` thành `.env` và điền các thông tin cấu hình cần thiết, đặc biệt là thông tin kết nối đến MS SQL Server.

### 2. Khởi Tạo Cơ Sở Dữ Liệu
Chạy lệnh này **một lần** để tạo các VIEW cần thiết trong DuckDB, giúp tối ưu hóa các truy vấn sau này.
```bash
python -m cli init-db
```

### 3. Chạy Quy Trình ETL
Để trích xuất dữ liệu từ SQL Server, biến đổi và nạp vào DuckDB, hãy chạy lệnh:
```bash
python -m cli run-etl
```
Chạy với 8 luồng
```bash
python -m cli run-etl --max-workers 8
```

### 4. Khởi Chạy Web Server
Để khởi động API và giao diện dashboard, sử dụng lệnh:
```bash
python -m cli serve
```
Bạn cũng có thể thay đổi host và port:
```bash
python -m cli serve --host 0.0.0.0 --port 8000
```
Ứng dụng sẽ có sẵn tại http://127.0.0.1:8000.

### 5. Để xem tất cả các lệnh có sẵn:
```bash
python -m cli --help
```

### 6. Phát Triển Frontend
Để tự động biên dịch các thay đổi về CSS trong quá trình phát triển, hãy chạy:
```bash
npm run css:watch
```

## Cấu Trúc Cơ Sở Dữ Liệu Nguồn (MS SQL Server)
```bash
dbo.store: Thông tin các cửa hàng/vị trí.
    ├── tid:            (int, not_null)
    └── name:           (char(80), not_null) ( Tên vị trí cửa đặt thiết bị )
dbo.ErrLog: Ghi nhận các lỗi từ thiết bị.
    ├── ID:             (bigint, not_null)
    ├── storeid:        (int, not_null) - (mapping dbo.store.tid)
    ├── DeviceCode:     (smallint, )
    ├── LogTime:        (datetime, )
    ├── Errorcode:      (int, )
    └── ErrorMessage:   (nchar(120), )
dbo.num_crowd: Dữ liệu đếm lượt ra/vào thô.
    ├── recordtime:     (datetime, )
    ├── in_num:         (int, not_null)
    ├── out_num:        (int, not_null)
    ├── position:       (char(30), )
    └── storeid:        (int, not_null) - (mapping dbo.store.tid)
dbo.Status: Dữ liệu trạng thái của thiết bị.
    ├── ID:             (int, not_null)
    ├── storeid:        (int, not_null) - (mapping dbo.store.tid)
    ├── FlashNum:       (int, )
    ├── RamNum:         (int, )
    ├── RC1:            (bit, )
    ├── RC2:            (bit, )
    ├── RC3:            (bit, )
    ├── RC4:            (bit, )
    ├── RC5:            (bit, )
    ├── RC6:            (bit, )
    ├── RC7:            (bit, )
    ├── RC8:            (bit, )
    ├── DcID:           (smallint, )
    ├── FV:             (nchar(20), )
    ├── DcTime:         (datetime, )
    ├── DeviceID:       (smallint, )
    ├── IA:             (int, )
    ├── OA:             (int, )
    ├── S:              (smallint, )
    └── T:              (datetime, )
```


#### Cài đặt thư viện Node.js & các gói phụ thuộc
```bash
npm init -y
npm install -D tailwindcss@3
npx tailwindcss init
npm run css:watch
npm run css:build
```

```
# Build image và khởi chạy container ở chế độ nền (-d)
docker-compose up --build -d

# Một vài lệnh hữu ích khác:
## Để xem log của ứng dụng: docker-compose logs -f
## Để dừng ứng dụng: docker-compose down

# Chạy ETL và tự động xóa container sau khi hoàn thành (--rm)
docker-compose run --rm web python -m cli run-etl
