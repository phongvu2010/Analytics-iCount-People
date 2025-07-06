"""
SQLAlchemy models for an existing MSSQL database using Manual Definition.
This script defines models explicitly to avoid reflection issues with certain drivers.
"""
import streamlit as st
from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, BigInteger, SmallInteger, Boolean
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from urllib import parse

# 1. CHUỖI KẾT NỐI (CONNECTION STRING)
# Driver 'ODBC Driver 17 for SQL Server' là phổ biến, hãy đảm bảo bạn đã cài đặt nó.
# Nếu bạn sử dụng Windows Authentication, chuỗi kết nối sẽ có dạng khác.
# Ví dụ Windows Auth: f'mssql+pyodbc://{db_host}:{db_port}/{db_name}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'
db_host = 'MSSQL'
db_port = 1433
db_name = 'statistic'
db_user = 'sa'
db_pass = parse.quote_plus('Admin@123')
db_driver = 'SQL+Server'

connection_string = f'mssql+pyodbc://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}?driver={db_driver}'

try:
    # 2. TẠO ENGINE
    # Engine là điểm khởi đầu cho mọi ứng dụng SQLAlchemy.
    engine = create_engine(connection_string)

    # 3. TẠO BASE CLASS CHO CÁC MODEL
    # Đây là lớp cơ sở mà các model của chúng ta sẽ kế thừa.
    Base = declarative_base()

    # 4. TẠO SESSION ĐỂ TRUY VẤN
    Session = sessionmaker(bind=engine)
    session = Session()

    # --- VÍ DỤ TRUY VẤN DỮ LIỆU (READ-ONLY) ---
    print('✅ Kết nối và ánh xạ database thành công!')
    print('\n--- Bắt đầu truy vấn dữ liệu mẫu ---')

    # Ví dụ 1: Lấy 5 cửa hàng đầu tiên từ bảng 'store'
    print('\n[INFO] Lấy 5 cửa hàng đầu tiên:')
    all_stores = session.query(Store).limit(5).all()
    if all_stores:
        for store_instance in all_stores:
            print(f'  - ID: {store_instance.tid}, Tên Cửa Hàng: {store_instance.name}, Mã code: {store_instance.code}')
    else:
        print('  - Không tìm thấy cửa hàng nào.')

    # Ví dụ 2: Lấy 5 log lỗi gần nhất và thông tin cửa hàng tương ứng
    print('\n[INFO] Lấy 5 log lỗi gần nhất và tên cửa hàng:')
    latest_logs = session.query(ErrLog).order_by(ErrLog.LogTime.desc()).limit(5).all()
    # # Câu query join vẫn hoạt động tương tự
    # latest_logs = session.query(ErrLog, Store)\
    #                      .join(Store, ErrLog.storeid == Store.tid)\
    #                      .order_by(ErrLog.LogTime.desc())\
    #                      .limit(10).all()

    if latest_logs:
        for log_instance in latest_logs:
            # Truy cập thông tin store thông qua relationship
            print(f"  - Log Time: {log_instance.LogTime.strftime('%Y-%m-%d %H:%M:%S')}, "
                  f"Cửa hàng: {log_instance.store.name}, "
                  f"Mã lỗi: {log_instance.Errorcode}")
    else:
        print('  - Không tìm thấy log lỗi nào.')

except Exception as e:
    print(f'❌ Đã xảy ra lỗi: {e}')
    print('\n--- GỢI Ý DEBUG ---')
    print('1. Kiểm tra lại chuỗi kết nối (user, password, server, database).')
    print('2. Đảm bảo driver ODBC cho SQL Server đã được cài đặt trên máy của bạn.')
    print('3. Kiểm tra tường lửa hoặc các quy tắc mạng có chặn kết nối đến SQL Server không.')

finally:
    # Luôn đóng session sau khi sử dụng xong để giải phóng tài nguyên.
    if 'session' in locals() and session.is_active:
        session.close()
        print('\n[INFO] Session đã được đóng.')
