// Component chứa bộ lọc (cửa hàng, ngày)
import React from 'react';
import { FormControl, InputLabel, Select, MenuItem, Stack, Paper } from '@mui/material';
import { DatePicker } from '@mui/x-date-pickers/DatePicker';

function Filters({ stores, filters, onFilterChange }) {
  const handleStoreChange = (event) => {
    onFilterChange({ ...filters, storeId: event.target.value });
  };

  const handleStartDateChange = (newDate) => {
    onFilterChange({ ...filters, startDate: newDate });
  };

  const handleEndDateChange = (newDate) => {
    onFilterChange({ ...filters, endDate: newDate });
  };

  return (
    <Paper elevation={2} sx={{ p: 2, mb: 3 }}>
      <Stack 
        direction={{ xs: 'column', sm: 'row' }} 
        spacing={2}
      >
        <FormControl sx={{ minWidth: 200, flex: 1 }}>
          <InputLabel id="store-select-label">Cửa hàng</InputLabel>
          <Select
            labelId="store-select-label"
            value={filters.storeId}
            label="Cửa hàng"
            onChange={handleStoreChange}
          >
            <MenuItem value=""><em>Tất cả cửa hàng</em></MenuItem>
            {stores.map((store) => (
              <MenuItem key={store.tid} value={store.tid}>
                {store.name}
              </MenuItem>
            ))}
          </Select>
        </FormControl>

        <DatePicker
          label="Từ ngày"
          value={filters.startDate}
          onChange={handleStartDateChange}
          sx={{ flex: 1 }}
        />
        <DatePicker
          label="Đến ngày"
          value={filters.endDate}
          onChange={handleEndDateChange}
          sx={{ flex: 1 }}
        />
      </Stack>
    </Paper>
  );
}

export default Filters;
