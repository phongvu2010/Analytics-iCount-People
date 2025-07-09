
# iCount People - Dashboard Thống kê

Đây là một ứng dụng web sử dụng FastAPI để hiển thị dashboard thống kê lượng người ra-vào một trung tâm thương mại, dữ liệu được lấy từ cơ sở dữ liệu MSSQL.

## Các công nghệ sử dụng

- **Backend**: FastAPI, Pydantic
- **Database Connector**: pyodbc
- **Data Processing**: Pandas, NumPy
- **Frontend**: HTML, Tailwind CSS, Chart.js
- **Server**: Uvicorn

## Cấu trúc dự án

```
iCount-People/
├── app/
│   ├── core/
│   │   ├── __init__.py
│   │   ├── config.py       # Cấu hình, đọc biến môi trường
│   │   └── db.py           # Xử lý kết nối và truy vấn DB
│   ├── statics/            # Chứa file tĩnh (logo, favicon)
│   ├── templates/
│   │   └── dashboard.html  # Template giao diện
│   ├── __init__.py
│   ├── main.py             # File khởi tạo ứng dụng FastAPI
│   ├── routers.py          # Định nghĩa các API endpoints
│   └── schemas.py          # Pydantic schemas để validate dữ liệu
├── .env                    # File chứa biến môi trường (cần tự tạo)
├── .gitignore
├── requirements.txt
└── README.md
```

## Hướng dẫn cài đặt và chạy

### 1. Yêu cầu tiên quyết

- Python 3.8+
- Đã cài đặt ODBC Driver for SQL Server. Bạn có thể tải từ [trang của Microsoft](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server).

### 2. Cài đặt

1.  **Clone repository:**
    ```bash
    git clone <your-repo-url>
    cd iCount-People
    ```

2.  **Tạo và kích hoạt môi trường ảo:**
    ```bash
    # Windows
    python -m venv venv
    .\venv\Scripts\activate

    # macOS / Linux
    python3 -m venv venv
    source venv/bin/activate
    ```

3.  **Cài đặt các thư viện cần thiết:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Tạo file `.env`:**
    Tạo một file tên là `.env` ở thư mục gốc của dự án. Sao chép nội dung từ file `.env (Ví dụ)` và điền thông tin kết nối CSDL MSSQL của bạn.

    ```
    DB_SERVER="your_server_name"
    DB_DATABASE="your_database_name"
    DB_USERNAME="your_username"
    DB_PASSWORD="your_password"
    DB_DRIVER="{ODBC Driver 17 for SQL Server}"
    ```

### 3. Chạy ứng dụng

Sử dụng Uvicorn để chạy server:

```bash
uvicorn app.main:app --reload
```

-   `--reload`: Tự động khởi động lại server mỗi khi có thay đổi trong code.

Sau khi server khởi động, mở trình duyệt và truy cập vào địa chỉ: `http://127.0.0.1:8000`

Bạn cũng có thể xem tài liệu API tự động được sinh ra bởi FastAPI tại: `http://127.0.0.1:8000/docs`

## Các tính năng chính

-   **Dashboard trực quan**: Hiển thị biểu đồ đường về lượng khách vào.
-   **Thống kê linh hoạt**: Xem dữ liệu theo ngày, tuần, tháng, và năm.
-   **Xử lý nhiễu**: Tự động lọc các giá trị tăng đột biến bất thường trong dữ liệu để biểu đồ chính xác và mượt mà hơn.
-   **Cảnh báo lỗi**: Hiển thị các lỗi mới nhất từ hệ thống đếm để kịp thời xử lý.
-   **API Backend**: Cung cấp các endpoint rõ ràng để lấy dữ liệu.


### Models DB
```
dbo.store
├── tid:            (int, not_null, auto_increment)
├── country:        (char(20), )
├── area:           (char(20), )
├── province:       (char(20), )
├── city:           (char(20), )
├── name:           (char(80), not_null)
├── address:        (char(80), not_null)
├── isbranch:       (char(3), )
├── code:           (char(32), not_null)
├── cameranum:      (int, not_null)
├── manager:        (char(20), )
├── managertel:     (char(20), )
├── lastEditDate:   (datetime, )
└── formula:        (char(64), )

dbo.ErrLog
├── ID:             (bigint, not_null, auto_increment)
├── storeid:        (int, not_null) - (mapping dbo.store.tid)
├── DeviceCode:     (smallint, )
├── LogTime:        (datetime, )
├── Errorcode:      (int, )
└── ErrorMessage:   (nchar(120), )

dbo.num_crowd
├── recordtime:     (datetime, )
├── in_num:         (int, not_null)
├── out_num:        (int, not_null)
├── position:       (char(30), )
└── storeid:        (int, not_null) - (mapping dbo.store.tid)

dbo.Status
├── ID:             (int, not_null, auto_increment)
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
