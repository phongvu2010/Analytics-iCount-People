### Sơ đồ cấu trúc dự án:
```
iCount/
├── app/
│   ├── api/                            # (Dành cho Giai đoạn 2) Chứa logic API của FastAPI
│   │   ├── routers/
│   │   ├── __init__.py
│   │   └── dependencies.py
│   ├── core/
│   │   └── config.py
│   ├── etl/
│   │   ├── __init__.py
│   │   ├── extract.py
│   │   ├── load.py
│   │   ├── state.py
│   │   └── transform.py
│   ├── schemas/                        # (Dành cho Giai đoạn 2) Định nghĩa các Pydantic model/schema
│   ├── utils/
│   │   └── logger.py
│   └── main.py                         # Entrypoint cho cả CLI (ETL) và chạy web server
├── configs/
│   ├── logger.yaml
│   └── tables.yaml
├── data/                               # Nơi lưu trữ file DuckDB
├── logs/                               # Nơi lưu trữ file log
├── template/
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


# iCount People Analytics
Hệ thống thống kê và phân tích lưu lượng người ra vào trung tâm thương mại, được xây dựng bằng FastAPI và DuckDB.

## Hướng dẫn cài đặt và chạy dự án
### 1. Yêu cầu
- Python 3.8+
- Dữ liệu Parquet của bạn được đặt trong thư mục `data/` theo đúng cấu trúc đã mô tả.

### 2. Các bước thiết lập
#### a. Sao chép dự án
Tạo một thư mục cho dự án và sao chép tất cả các file và thư mục đã được cung cấp ở trên vào đó.

#### b. Tạo môi trường ảo (Khuyến khích)
```
python -m venv venv
source venv/bin/activate  # Trên Windows: venv\Scripts\activate
```

#### c. Cài đặt các thư viện cần thiết
```
pip install -r requirements.txt
```

#### d. Chuẩn bị dữ liệu
Đảm bảo rằng bạn có thư mục `data/` ở cùng cấp với thư mục `app/` và chứa dữ liệu Parquet của bạn theo cấu trúc:
```
iCount-People/
├── app/
├── data/
│   ├── crowd_counts/
│   │   └── year=.../*.parquet
│   └── error_logs/
│       └── year=.../*.parquet
├── .env
├── requirements.txt
└── ...
```

#### e. Chuẩn bị file tĩnh
Tạo các file `logo.png` và `favicon.ico` trong thư mục `app/static/` để logo và icon của trang web hiển thị.

### 3. Chạy ứng dụng
Sử dụng Uvicorn để khởi động server. Từ thư mục gốc của dự án (`iCount-People/`), chạy lệnh sau:
```
uvicorn app.main:app --reload
```
- `app.main:app`: Chỉ cho Uvicorn tìm đối tượng `app` trong file `app/main.py`.
- `--reload`: Tự động khởi động lại server mỗi khi có thay đổi trong code.

### 4. Truy cập ứng dụng
Sau khi server khởi động, bạn có thể:
- Truy cập Dashboard: Mở trình duyệt và đi đến http://127.0.0.1:8000
- Xem tài liệu API: Truy cập http://127.0.0.1:8000/docs để xem giao diện Swagger UI tương tác.

Chúc bạn thành công với dự án! Nếu có bất kỳ câu hỏi nào khác, đừng ngần ngại hỏi nhé.
