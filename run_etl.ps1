# Lấy ngày giờ hiện tại để ghi log
$Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
Write-Output "[$Timestamp] Starting iCount People ETL process..."

# --- QUAN TRỌNG: Thay đổi đường dẫn này ---
# Thay 'C:\path\to\your\project' bằng đường dẫn THỰC TẾ đến thư mục dự án của bạn
$ProjectDirectory = "C:\path\to\your\project"

try {
    # Di chuyển vào thư mục gốc của dự án
    Set-Location -Path $ProjectDirectory

    # Chạy lệnh ETL bằng docker-compose
    # Chúng ta bắt output và error stream để kiểm tra
    $output = docker-compose run --rm web python -m cli run-etl 2>&1

    # In kết quả ra màn hình (và file log nếu có)
    Write-Output $output

    $Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Write-Output "[$Timestamp] ETL process finished."
}
catch {
    # Bắt lỗi nếu có sự cố
    $Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Write-Error "[$Timestamp] An error occurred during the ETL process:"
    Write-Error $_.Exception.Message
}
