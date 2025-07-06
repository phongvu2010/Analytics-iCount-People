// Component layout chung (thanh điều hướng, footer)
import React from 'react';
import { AppBar, Toolbar, Typography, Box, Container } from '@mui/material';
import AssessmentIcon from '@mui/icons-material/Assessment'; // Icon cho đẹp

// Layout sẽ nhận các component con (children) và hiển thị bên trong nó
function Layout({ children }) {
  return (
    <Box sx={{ display: 'flex' }}>
      <AppBar position="fixed">
        <Toolbar>
          <AssessmentIcon sx={{ mr: 2 }} />
          <Typography variant="h6" noWrap component="div">
            iCount People Application
          </Typography>
        </Toolbar>
      </AppBar>

      {/* Container chính cho nội dung trang */}
      <Container component="main" sx={{ mt: '80px', flexGrow: 1, p: 3 }}>
        {children}
      </Container>
    </Box>
  );
}

export default Layout;
