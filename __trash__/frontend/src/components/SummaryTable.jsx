// Component bảng tổng hợp
import React from 'react';
import { Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Paper, Typography } from '@mui/material';

function SummaryTable({ data }) {
  if (!data || data.length === 0) {
    return (
      <Paper elevation={3} sx={{ p: 2, height: '448px', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <Typography>Không có dữ liệu tổng hợp.</Typography>
      </Paper>
    );
  }

  return (
    <TableContainer component={Paper} elevation={3} sx={{ height: '448px' }}>
      <Table stickyHeader aria-label="Bảng tổng hợp">
        <TableHead>
          <TableRow>
            <TableCell sx={{ fontWeight: 'bold' }}>Ngày</TableCell>
            <TableCell align="right" sx={{ fontWeight: 'bold' }}>Tổng vào</TableCell>
            <TableCell align="right" sx={{ fontWeight: 'bold' }}>Tổng ra</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {data.map((row) => (
            <TableRow key={row.date} sx={{ '&:hover': { backgroundColor: '#fafafa' } }}>
              <TableCell component="th" scope="row">{row.date}</TableCell>
              <TableCell align="right">{row.in_num}</TableCell>
              <TableCell align="right">{row.out_num}</TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </TableContainer>
  );
}

export default SummaryTable;
