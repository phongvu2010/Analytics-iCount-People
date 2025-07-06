// Cấu hình Axios và định nghĩa các hàm gọi API
import axios from 'axios';

// Tạo một instance của axios với cấu hình mặc định
const apiClient = axios.create({
  baseURL: 'http://127.0.0.1:8000/api/v1', // Địa chỉ API backend
//   baseURL: 'https://1tlw4xrk-8000.asse.devtunnels.ms/api/v1', // Địa chỉ API backend
  headers: {
    'Content-Type': 'application/json',
  },
});

// Định nghĩa các hàm gọi API
export const getStores = () => apiClient.get('/stores/');

export const getCrowdData = (params) => apiClient.get('/crowds/', { params });
// ví dụ params: { start_date: '2025-07-06', end_date: '2025-07-07', store_id: 1 }

export const getRecentErrors = () => apiClient.get('/errors/recent');
