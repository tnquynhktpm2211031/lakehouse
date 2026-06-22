import React, { useState } from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import Login from './pages/Login';
import UserUpload from './pages/UserUpload';
import AdminDashboard from './pages/AdminDashboard';

function App() {
  // Quản lý trạng thái đăng nhập
  const [token, setToken] = useState(localStorage.getItem('token') || '');
  const [role, setRole] = useState(localStorage.getItem('role') || '');

  return (
    <Router>
      <Routes>
        {/* Trang chủ chuyển hướng thẳng sang Login */}
        <Route path="/" element={<Login setToken={setToken} setRole={setRole} />} />
        <Route path="/login" element={<Login setToken={setToken} setRole={setRole} />} />
        
        {/* Phân quyền Router */}
        <Route path="/user" element={token ? <UserUpload /> : <Navigate to="/login" />} />
        <Route path="/admin" element={token && role === 'admin' ? <AdminDashboard /> : <Navigate to="/login" />} />
        
        {/* Redirect nếu sai đường dẫn */}
        <Route path="*" element={<Navigate to="/" />} />
      </Routes>
    </Router>
  );
}

export default App;