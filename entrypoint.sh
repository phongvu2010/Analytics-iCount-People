#!/bin/bash

# Chờ một chút để đảm bảo các service khác (nếu có) sẵn sàng, mặc dù ở đây không cần thiết lắm.
sleep 2

# Luôn chạy ETL trước tiên.
# Lần đầu tiên, nó sẽ tạo database và load toàn bộ dữ liệu.
# Những lần sau, nó sẽ chỉ load dữ liệu mới (incremental).
echo "========= RUNNING ETL PROCESS ========="
python -m cli run-etl --clear-cache=false # Tắt clear-cache vì server chưa chạy

# Sau khi ETL chạy và tạo các bảng, bây giờ chúng ta có thể khởi tạo VIEW.
# Lệnh này giờ sẽ luôn thành công.
echo "========= INITIALIZING DATABASE VIEWS ========="
python -m cli init-db

# Cuối cùng, thực thi lệnh chính được truyền vào từ Dockerfile (chính là lệnh khởi động server)
echo "========= STARTING FASTAPI SERVER ========="
exec "$@"
