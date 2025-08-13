
# iCount People Analytics
Hệ thống thống kê và phân tích lưu lượng người ra vào trung tâm thương mại, được xây dựng bằng FastAPI và DuckDB.

### Sơ đồ cấu trúc dự án ( Phương án rút gọn ):
```
iCount/
├── app/
│   ├── core/
│   │   ├── caching.py
│   │   ├── config.py
│   │   └── data_handler.py
│   ├── etl/
│   │   ├── __init__.py
│   │   ├── extract.py
│   │   ├── load.py
│   │   ├── schemas.py
│   │   ├── state.py
│   │   └── transform.py
│   ├── utils/
│   │   └── logger.py
│   ├── main.py
│   ├── routers.py
│   ├── schemas.py
│   └── services.py
├── configs/
│   ├── logger.yaml
│   └── tables.yaml
├── data/                               # Nơi lưu trữ file DuckDB
├── logs/                               # Nơi lưu trữ file log
├── template/                           # (Dành cho Giai đoạn 3)
│   ├── partials/
│   │   ├── _charts.html                # Phần chứa các biểu đồ
│   │   ├── _error_modal.html           # Phần chứa các error modal
│   │   ├── _filters.html               # Phần chứa các bộ lọc
│   │   ├── _header.html                # Phần header của trang
│   │   ├── _metrics.html               # Phần chứa các thẻ chỉ số
│   │   ├── _scripts.html               # Toàn bộ code JavaScript
│   │   ├── _sidebar.html               # Phần chứa các sidebar
│   │   └── _table.html                 # Phần bảng dữ liệu chi tiết
│   ├── statics/
│   │   ├── css/
│   │   │   └── style.css
│   │   ├── js/
│   │   │   └── dashboard.js
│   │   ├── favicon.ico
│   │   └── logo.png
│   ├── base.html                       # File layout chính, chứa cấu trúc chung
│   └── dashboard.html                  # File nội dung chính, kế thừa từ base.html
├── tests/                              # Thư mục chứa các bài test
│   ├── etl/
│   │   ├── __init__.py
│   │   └── test_transform.py
│   └── __init__.py
├── .env
├── .env.example
├── .gitignore
├── cli.py
├── pyproject.toml                      # Thay cho requirements.txt để quản lý dependency tốt hơn
└── README.md
```

### Sơ đồ cấu trúc dự án ( Phương án mở rộng ):
```
iCount/
├── app/
│   ├── api/                            # (Dành cho Giai đoạn 2) Chứa logic API của FastAPI
│   │   ├── routers/
│   │   ├── __init__.py
│   │   └── dependencies.py
│   ├── core/
│   │   ├── caching.py
│   │   ├── config.py
│   │   └── data_handler.py
│   ├── etl/
│   │   ├── __init__.py
│   │   ├── extract.py
│   │   ├── load.py
│   │   ├── schemas.py
│   │   ├── state.py
│   │   └── transform.py
│   ├── schemas/                        # (Dành cho Giai đoạn 2) Định nghĩa các Pydantic model/schemas
│   ├── utils/
│   │   └── logger.py
│   └── main.py                         # Entrypoint cho ứng dụng web FastAPI
├── configs/
│   ├── logger.yaml
│   └── tables.yaml
├── data/                               # Nơi lưu trữ file DuckDB
├── logs/                               # Nơi lưu trữ file log
├── template/                           # (Dành cho Giai đoạn 3)
│   ├── partials/
│   │   ├── _charts.html                # Phần chứa các biểu đồ
│   │   ├── _error_modal.html           # Phần chứa các error modal
│   │   ├── _filters.html               # Phần chứa các bộ lọc
│   │   ├── _header.html                # Phần header của trang
│   │   ├── _metrics.html               # Phần chứa các thẻ chỉ số
│   │   ├── _scripts.html               # Toàn bộ code JavaScript
│   │   ├── _sidebar.html               # Phần chứa các sidebar
│   │   └── _table.html                 # Phần bảng dữ liệu chi tiết
│   ├── statics/
│   │   ├── css/
│   │   │   └── style.css
│   │   ├── js/
│   │   │   └── dashboard.js
│   │   ├── favicon.ico
│   │   └── logo.png
│   ├── base.html                       # File layout chính, chứa cấu trúc chung
│   └── dashboard.html                  # File nội dung chính, kế thừa từ base.html
├── tests/                              # Thư mục chứa các bài test
│   ├── etl/
│   │   ├── __init__.py
│   │   └── test_transform.py
│   └── __init__.py
├── .env
├── .env.example
├── .gitignore
├── cli.py                              # Entrypoint cho cả CLI (ETL) và chạy web server
├── pyproject.toml                      # Thay cho requirements.txt để quản lý dependency tốt hơn
└── README.md
```

### Cấu trúc DB SQL Server
```
dbo.store: Bảng ghi vị trí thiết bị cửa ra vào
├── tid:            (int, not_null)
└── name:           (char(80), not_null) ( Tên vị trí cửa đặt thiết bị )

dbo.ErrLog: Bảng ghi lỗi hệ thống thiết bị
├── ID:             (bigint, not_null)
├── storeid:        (int, not_null) - (mapping dbo.store.tid)
├── DeviceCode:     (smallint, )
├── LogTime:        (datetime, )
├── Errorcode:      (int, )
└── ErrorMessage:   (nchar(120), )

dbo.num_crowd: Bảng đếm dữ liệu ra vào tại thiết bị 
├── recordtime:     (datetime, )
├── in_num:         (int, not_null)
├── out_num:        (int, not_null)
├── position:       (char(30), )
└── storeid:        (int, not_null) - (mapping dbo.store.tid)

dbo.Status
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

#### Để chạy quy trình ETL:
```
python -m cli run-etl
```
#### Để chạy web server:
```
python -m cli serve
```
#### Bạn cũng có thể thay đổi host và port:
```
python -m cli serve --host 0.0.0.0 --port 8888
```
#### Để xem tất cả các lệnh có sẵn:
```
python -m cli --help
```
