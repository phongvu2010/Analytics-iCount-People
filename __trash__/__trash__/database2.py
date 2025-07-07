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




# import pandas as pd
# import sqlalchemy
# from datetime import date
# from sqlalchemy import create_engine, extract, MetaData
# from sqlalchemy.ext.automap import automap_base

# from sqlalchemy.orm import sessionmaker, Session
# from models import Store, NumCrowd, ErrLog, Status

# @st.cache_data(ttl = 86400, show_spinner = False)
# def dbStore():
#     query = getSession().query(Store)

#     return pd.read_sql(sql = query.statement, con = engine)
#     # return pd.DataFrame([r._asdict() for r in results])

# @st.cache_data(ttl = 900, show_spinner = False)
# def dbNumCrowd(year = None):
#     query = getSession().query(NumCrowd)
#     if year: query = query.filter(extract('year', NumCrowd.recordtime) == year)

#     return pd.read_sql(sql = query.statement, con = engine)

# @st.cache_data(ttl = 3600, show_spinner = False)
# def dbErrLog():
#     query = getSession().query(ErrLog).order_by(ErrLog.LogTime.desc()).limit(500)

#     return pd.read_sql(sql = query.statement, con = engine)

# @st.cache_data(ttl = 3600, show_spinner = False)
# def dbStatus():
#     query = getSession().query(Status)

#     return pd.read_sql(sql = query.statement, con = engine)




