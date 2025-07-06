// Component biểu đồ
import React from 'react';
import ReactECharts from 'echarts-for-react';
import { Paper, Typography } from '@mui/material';

function CrowdChart({ data }) {
  if (!data || data.length === 0) {
    return (
      <Paper elevation={3} sx={{ p: 2, height: '448px', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <Typography>Không có dữ liệu cho biểu đồ.</Typography>
      </Paper>
    );
  }

  const hours = data.map(item => new Date(item.hour).toLocaleTimeString('vi-VN', { hour: '2-digit', minute: '2-digit' }));
  const inData = data.map(item => item.in_num);
  const outData = data.map(item => item.out_num);

  const option = {
    title: { text: 'Biểu đồ lưu lượng ra vào theo giờ', left: 'center' },
    tooltip: { trigger: 'axis' },
    legend: { data: ['Lượt vào', 'Lượt ra'], bottom: 10 },
    grid: { left: '3%', right: '4%', bottom: '10%', containLabel: true },
    xAxis: { type: 'category', boundaryGap: false, data: hours },
    yAxis: { type: 'value', name: 'Số lượng' },
    series: [
      { name: 'Lượt vào', type: 'line', smooth: true, data: inData, itemStyle: { color: '#5470C6' } },
      { name: 'Lượt ra', type: 'line', smooth: true, data: outData, itemStyle: { color: '#91CC75' } }
    ]
  };

  return (
    <Paper elevation={3} sx={{ p: 2 }}>
      <ReactECharts option={option} style={{ height: '400px', width: '100%' }} />
    </Paper>
  );
}

export default CrowdChart;
