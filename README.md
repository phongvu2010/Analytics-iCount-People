iCount-Project/
├── icount-api/                     # Backend FastAPI
│   ├── app/
│   │   ├── __init__.py
│   │   ├── main.py                 # (1) Khởi tạo app FastAPI và các router
│   │   ├── api/
│   │   │   ├── __init__.py
│   │   │   ├── deps.py             # (2) Dependencies, ví dụ: hàm get_current_user
│   │   │   └── routers/
│   │   │       ├── auth.py         # (3) Router cho đăng nhập, tạo token
│   │   │       ├── data.py         # (3) Router cho dữ liệu đếm người (public)
│   │   │       └── logs.py         # (3) Router cho log lỗi (private)
│   │   ├── core/
│   │   │   ├── __init__.py
│   │   │   ├── config.py           # (4) Chứa các biến môi trường, cấu hình
│   │   │   └── database.py         # (5) Thiết lập kết nối database
│   │   ├── crud/
│   │   │   ├── __init__.py
│   │   │   └── crud.py             # (6) Các hàm CRUD (lấy, tạo, sửa, xóa) dữ liệu
│   │   ├── models/
│   │   │   ├── __init__.py
│   │   │   └── models.py           # (7) Định nghĩa các bảng DB bằng SQLAlchemy
│   │   ├── schemas/
│   │   │   ├── __init__.py
│   │   │   └── schemas.py          # (8) Định nghĩa cấu trúc dữ liệu API bằng Pydantic
│   │   └── services/
│   │       ├── __init__.py
│   │       └── analysis.py         # (9) Logic phân tích dữ liệu bằng Pandas
│   ├── tests/                      # Thư mục chứa các file test
│   ├── .env                        # File chứa biến môi trường (không commit lên Git)
│   ├── .gitignore
│   └── requirements.txt            # Các thư viện Python cần thiết
├── icount-web/                     # Frontend React/Vue
│   ├── public/                     # Chứa file index.html và các tài nguyên tĩnh
│   │   ├── index.html
│   │   └── favicon.ico
│   ├── src/
│   │   ├── api/                    # (A) Chứa logic gọi API từ Backend
│   │   │   └── apiClient.js        # (Cấu hình Axios/Fetch)
│   │   ├── assets/                 # (B) Chứa fonts, images, css global
│   │   │   └── styles/
│   │   ├── components/             # (C) Các component UI tái sử dụng
│   │   │   ├── common/             # (VD: Button, Input, Modal)
│   │   │   ├── layout/             # (VD: Navbar, Sidebar, Footer)
│   │   │   └── charts/             # (VD: LineChart, BarChart)
│   │   ├── contexts/               # (D) Quản lý state global bằng Context API
│   │   │   └── AuthContext.js      # (VD: Quản lý trạng thái đăng nhập)
│   │   ├── hooks/                  # (E) Chứa các custom hooks
│   │   │   └── useApi.js           # (VD: Hook để gọi API, quản lý loading, error)
│   │   ├── pages/                  # (F) Mỗi file là một trang của ứng dụng
│   │   │   ├── DashboardPage.js
│   │   │   ├── LoginPage.js
│   │   │   ├── ErrorLogPage.js
│   │   │   └── NotFoundPage.js
│   │   ├── App.js                  # (G) Component gốc, định tuyến (routing)
│   │   ├── index.js                # Điểm bắt đầu của ứng dụng React
│   │   └── routes.js               # (H) Định nghĩa các route của ứng dụng
│   ├── .gitignore
│   └── package.json
└── README.md
