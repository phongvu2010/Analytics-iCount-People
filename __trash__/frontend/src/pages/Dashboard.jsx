// Trang chính, nơi lắp ráp các component lại
import React, { useState, useEffect } from 'react';
// import { Box, Grid, CircularProgress, Typography } from '@mui/material';
import { Box, Grid, Typography, CircularProgress } from '@mui/material';
import Filters from '../components/Filters';
import CrowdChart from '../components/CrowdChart';
import SummaryTable from '../components/SummaryTable';
import * as apiClient from '../api/apiClient';

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
    }).catch(error => {
      console.error("Lỗi khi tải danh sách cửa hàng:", error);
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
      .catch(error => {
        console.error("Lỗi khi tải dữ liệu đếm người:", error);
        setCrowdData(null); // Reset data nếu có lỗi
      })
      .finally(() => {
        setLoading(false);
      });
  }, [filters]);

  return (
    <Box>
      <Filters stores={stores} filters={filters} onFilterChange={setFilters} />
      
      <Grid container spacing={3}>
        {loading ? (
          <Grid item xs={12} sx={{ display: 'flex', justifyContent: 'center', mt: 5 }}>
            <CircularProgress />
          </Grid>
        ) : crowdData ? (
          <>
            <Grid item xs={12} lg={8}>
              <CrowdChart data={crowdData.summary_by_hour} />
            </Grid>
            <Grid item xs={12} lg={4}>
              <SummaryTable data={crowdData.summary_by_day} />
            </Grid>
          </>
        ) : (
          <Grid item xs={12}>
            <Typography sx={{ textAlign: 'center', mt: 5 }}>Không có dữ liệu để hiển thị.</Typography>
          </Grid>
        )}
      </Grid>
    </Box>
  );
}

export default Dashboard;
