  import React, { useState } from 'react';
  import { useNavigate } from 'react-router-dom';
  import CatalogHistoryTimeline from './CatalogHistoryTimeline';

  const AdminDashboard = () => {
    const navigate = useNavigate();
    const [activeTab, setActiveTab] = useState('dashboard');
    
    const supersetUrl = import.meta.env.VITE_SUPERSET_DASHBOARD_URL || "http://localhost:8088/superset/dashboard/1/?native_filters_key=8TNRVjTeKm37iM9LWUB6EX-Z6hUKzsb-3BK6RuTaYCHmOLIwd75IMSdjyh913EeT&standalone=3";

    const handleLogout = () => {
      localStorage.removeItem('token');
      localStorage.removeItem('role');
      navigate('/login');
    };

    return (
      <div className="flex h-screen bg-gray-100 font-sans">
        {/* Sidebar */}
        <div className="w-64 bg-slate-900 text-white flex flex-col shadow-2xl">
          <div className="h-20 flex items-center justify-center border-b border-slate-800">
            <h1 className="text-2xl font-bold text-transparent bg-clip-text bg-gradient-to-r from-blue-400 to-teal-300">
              Lakehouse
            </h1>
          </div>

          <nav className="flex-1 p-4 space-y-2 mt-4">
            <div className="text-xs text-slate-400 uppercase font-semibold mb-2">Quản trị hệ thống</div>

            {/* Chuyển <a> thành <button> hoặc <div> để hết cảnh báo */}
            <button 
              onClick={() => setActiveTab('dashboard')}
              className={`w-full text-left py-3 px-4 rounded transition ${activeTab === 'dashboard' ? 'bg-blue-600 shadow hover:bg-blue-700' : 'hover:bg-slate-800'}`}>
              📊 Báo cáo Tổng hợp (Gold)
            </button>
            <button 
              onClick={() => setActiveTab('pipeline')}
              className={`w-full text-left py-3 px-4 rounded transition ${activeTab === 'pipeline' ? 'bg-blue-600 shadow hover:bg-blue-700' : 'hover:bg-slate-800'}`}>
              ⚙️ Trạng thái Pipeline
            </button>
            <button className="w-full text-left py-3 px-4 rounded hover:bg-slate-800 transition">
              👥 Quản lý Người dùng
            </button>
          </nav>

          <div className="p-6 border-t border-slate-800">
            <div className="flex items-center space-x-3 mb-4">
              <div className="w-10 h-10 rounded-full bg-teal-500 flex items-center justify-center font-bold text-lg">A</div>
              <div>
                <p className="text-sm font-medium">Ban Giám Hiệu</p>
                <p className="text-xs text-slate-400">Admin</p>
              </div>
            </div>
            <button
              onClick={handleLogout}
              className="w-full text-center py-2 bg-slate-800 hover:bg-red-600 text-slate-300 hover:text-white rounded transition"
            >
              Đăng xuất
            </button>
          </div>
        </div>

        {/* Main Content */}
        <div className="flex-1 flex flex-col overflow-hidden">
          <header className="h-20 bg-white shadow-sm flex items-center justify-between px-8 z-10">
            <h2 className="text-xl font-semibold text-gray-800">Dashboard Khai thác Dữ liệu</h2>
            <div className="text-sm text-gray-500">Kết nối trực tiếp: <span className="text-green-600 font-bold">Apache Superset & Trino</span></div>
          </header>

          <main className="flex-1 overflow-hidden p-6 bg-gray-50">
            <div className="bg-white rounded-xl shadow-sm border border-gray-200 h-full overflow-hidden flex flex-col">
              <div className="p-4 border-b bg-gray-50 flex justify-between items-center">
                <h3 className="font-medium text-gray-700">
                  {activeTab === 'dashboard' ? 'Báo cáo Thường niên chất lượng Giáo dục' : 'Lịch sử Pipeline (Nessie)'}
                </h3>
                <button className="text-sm text-blue-600 hover:underline">Mở toàn màn hình</button>
              </div>
              <div className="flex-1 bg-gray-100 flex items-center justify-center overflow-hidden">
                {activeTab === 'dashboard' ? (
                  <iframe
                    src={supersetUrl}
                    title="Superset Dashboard"
                    className="w-full h-full border-0"
                  />
                ) : (
                  <CatalogHistoryTimeline />
                )}
              </div>
            </div>
          </main>
        </div>
      </div>
    );
  };

  export default AdminDashboard;
