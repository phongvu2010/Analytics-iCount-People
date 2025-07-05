import pyodbc

try:
    # Lấy danh sách tất cả các driver
    installed_drivers = pyodbc.drivers()

    print("✅ Các ODBC driver đã được cài đặt trên máy:")

    # In ra từng driver cho dễ nhìn
    for driver in installed_drivers:
        print(driver)
except Exception as ex:
    print(f"Lỗi khi liệt kê driver: {ex}")
