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

sudo docker-compose up --build -d
sudo docker-compose exec api /home/appuser/.venv/bin/python cli.py run-etl
sudo docker-compose exec api /home/appuser/.venv/bin/python cli.py init-db