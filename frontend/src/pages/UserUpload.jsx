import React, { useState, useRef } from 'react'; // 1. Import thêm useRef
import { useNavigate } from 'react-router-dom';
import axios from 'axios';

const UserUpload = () => {
  const navigate = useNavigate();
  const fileInputRef = useRef(null); // 2. Tạo ref cho input
  const [isDragging, setIsDragging] = useState(false);
  const [uploadStatus, setUploadStatus] = useState('');

  const handleLogout = () => {
    localStorage.removeItem('token');
    localStorage.removeItem('role');
    navigate('/login');
  };

  // Hàm xử lý chung khi có file (dù là kéo thả hay click chọn)
  const processFile = async (file) => {
    if (!file) return;
    const formData = new FormData();
    formData.append("file", file);

    try {
      setUploadStatus(`Đang tải ${file.name} lên Lakehouse...`);
      const res = await axios.post("http://localhost:8000/api/upload/", formData, {
        headers: { 
          "Content-Type": "multipart/form-data",
          "Authorization": `Bearer ${localStorage.getItem('token')}`
        }
      });
      setUploadStatus(`✅ Thành công: ${res.data.message}`);
    } catch (error) {
      setUploadStatus("❌ Lỗi tải lên: Bạn không có quyền hoặc server không phản hồi.");
    }
  };

  const onDragOver = (e) => { e.preventDefault(); setIsDragging(true); };
  const onDragLeave = (e) => { e.preventDefault(); setIsDragging(false); };
  const onDrop = async (e) => {
    e.preventDefault();
    setIsDragging(false);
    if (e.dataTransfer.files.length > 0) processFile(e.dataTransfer.files[0]);
  };

  return (
    <div className="min-h-screen bg-gray-50 font-sans">
      <header className="bg-white shadow px-8 py-4 flex justify-between items-center">
        <h1 className="text-2xl font-bold text-blue-600">EduLakehouse - Cổng Dịch Vụ Nạp Dữ Liệu</h1>
        <button onClick={handleLogout} className="px-4 py-2 bg-red-100 text-red-600 rounded">Đăng xuất</button>
      </header>

      <main className="max-w-4xl mx-auto mt-10 p-6">
        <div className="bg-white rounded-2xl shadow-xl p-10 border border-gray-100">
          <h2 className="text-xl font-bold text-gray-800 mb-2">Nạp Báo cáo Thường niên (Bronze Layer)</h2>
          
          {/* 3. Input ẩn để chọn file */}
          <input 
            type="file" 
            ref={fileInputRef} 
            onChange={(e) => processFile(e.target.files[0])} 
            className="hidden" 
          />

          {/* 4. Click vào div sẽ kích hoạt input */}
          <div 
            onClick={() => fileInputRef.current.click()}
            onDragOver={onDragOver}
            onDragLeave={onDragLeave}
            onDrop={onDrop}
            className={`border-4 border-dashed rounded-xl p-16 text-center transition-all duration-300 cursor-pointer ${
              isDragging ? 'border-blue-500 bg-blue-50' : 'border-gray-300 hover:bg-gray-50'
            }`}
          >
            <div className="text-6xl mb-4">{isDragging ? '📥' : '📄'}</div>
            <p className="text-lg font-medium text-gray-700">
              Kéo & Thả hoặc Click để chọn file
            </p>
            <p className="text-sm text-gray-400 mt-2">Định dạng hỗ trợ: PDF, DOCX</p>
          </div>

          {uploadStatus && (
            <div className="mt-8 p-4 bg-blue-50 text-blue-700 rounded-lg text-center font-medium">
              {uploadStatus}
            </div>
          )}
        </div>
      </main>
    </div>
  );
};

export default UserUpload;