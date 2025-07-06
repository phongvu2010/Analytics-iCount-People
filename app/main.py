import streamlit as st
from database import ReadOnlySessionFactory

# Tạo một session chỉ đọc
read_only_session = ReadOnlySessionFactory()

try:
    # 1. Lấy tất cả cửa hàng
    all_stores = read_only_session.query(Store).all()
    st.write("--- Danh sách cửa hàng ---")
    for store in all_stores:
        st.write(f"ID: {store.tid}, Tên: {store.name}")

finally:
    # Đóng session sau khi sử dụng xong
    read_only_session.close()
