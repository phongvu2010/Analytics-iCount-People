// # Trang chính, nơi lắp ráp các component lại
import React, { useState, useEffect } from 'react';
import { Box, Grid, Typography, CircularProgress } from '@mui/material';
import Filters from '../components/Filters';
import CrowdChart from '../components/CrowdChart';
import SummaryTable from '../components/SummaryTable';
import * as apiClient from '../api/v1/apiClient';

function Dashboard() {
  const [filters, setFilters] = useState({
    storeId: '', // '' nghĩa là "Tất cả"
    startDate: new Date(),
    endDate: new Date(),
  });
  const [stores, setStores] = useState([]);
  const [crowdData, setCrowdData] = useState(null);
  const [loading, setLoading] = useState(false);

  // Lấy danh sách cửa hàng khi load trang
  useEffect(() => {
    apiClient.getStores().then(response => {
      setStores(response.data);
    });
  }, []);

  // Lấy dữ liệu đếm người khi bộ lọc thay đổi
  useEffect(() => {
    setLoading(true);
    const params = {
      start_date: filters.startDate.toISOString().split('T')[0],
      end_date: filters.endDate.toISOString().split('T')[0],
      store_id: filters.storeId || null,
    };
    apiClient.getCrowdData(params)
      .then(response => {
        setCrowdData(response.data);
      })
      .finally(() => {
        setLoading(false);
      });
  }, [filters]);

  return (
    <Box sx={{ flexGrow: 1, p: 3 }}>
      <Typography variant="h4" gutterBottom>iCount People Dashboard</Typography>
      
      <Grid container spacing={3}>
        {/* Bộ lọc */}
        <Grid item xs={12}>
          <Filters stores={stores} filters={filters} onFilterChange={setFilters} />
        </Grid>

        {/* Biểu đồ và Bảng */}
        {loading ? (
          <Grid item xs={12} sx={{ display: 'flex', justifyContent: 'center', mt: 4 }}>
            <CircularProgress />
          </Grid>
        ) : crowdData && (
          <>
            <Grid item xs={12} md={8}>
              <CrowdChart data={crowdData.summary_by_hour} />
            </Grid>
            <Grid item xs={12} md={4}>
              <SummaryTable data={crowdData.summary_by_day} />
            </Grid>
          </>
        )}
      </Grid>
    </Box>
  );
}

export default Dashboard;
