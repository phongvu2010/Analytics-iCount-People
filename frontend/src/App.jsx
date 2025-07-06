import React from 'react';
import { createTheme, ThemeProvider } from '@mui/material/styles';
import CssBaseline from '@mui/material/CssBaseline';
import { LocalizationProvider } from '@mui/x-date-pickers';
import { AdapterDateFns } from '@mui/x-date-pickers/AdapterDateFns';

import Dashboard from './pages/Dashboard';
import Layout from './components/Layout';

// Tạo một theme MUI cơ bản
const theme = createTheme({
  palette: {
    mode: 'dark', // or 'light'
  },
});

function App() {
  return (
    <ThemeProvider theme={theme}>
      <LocalizationProvider dateAdapter={AdapterDateFns}>
        {/* CssBaseline giúp chuẩn hóa CSS trên các trình duyệt */}
        <CssBaseline />
        <Layout>
          <Dashboard />
        </Layout>
      </LocalizationProvider>
    </ThemeProvider>
  );
}

export default App;
