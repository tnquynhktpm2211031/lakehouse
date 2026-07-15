// import React, { useState } from 'react';

// const UploadComponent = () => {
//     const [uploadStatus, setUploadStatus] = useState('');
//     const [isDragging, setIsDragging] = useState(false);

//     // Xử lý khi kéo file vào vùng
//     const onDragOver = (e) => {
//         e.preventDefault();
//         setIsDragging(true);
//     };

//     // Xử lý khi kéo file ra khỏi vùng
//     const onDragLeave = (e) => {
//         e.preventDefault();
//         setIsDragging(false);
//     };

//     // Xử lý khi thả file xuống
//     const onDrop = async (e) => {
//         e.preventDefault();
//         setIsDragging(false);
        
//         const files = e.dataTransfer.files;
//         if (files.length === 0) return;

//         // Xử lý upload từng file
//         for (let i = 0; i < files.length; i++) {
//             await uploadFile(files[i]);
//         }
//     };

//     // Hàm gọi API đẩy file xuống Backend
//     const uploadFile = async (file) => {
//         const formData = new FormData();
//         formData.append("file", file);

//         try {
//             setUploadStatus(`Đang tải lên ${file.name}...`);
//             const response = await fetch("http://localhost:8000/api/upload-to-bronze/", {
//                 method: "POST",
//                 body: formData,
//             });

//             if (response.ok) {
//                 setUploadStatus(`Thành công: Đã lưu ${file.name} vào Data Lakehouse!`);
//             } else {
//                 setUploadStatus(`Thất bại khi tải ${file.name}`);
//             }
//         } catch (error) {
//             console.error("Lỗi:", error);
//             setUploadStatus("Có lỗi kết nối đến Backend.");
//         }
//     };

//     return (
//         <div style={{ padding: '50px', fontFamily: 'Arial' }}>
//             <h2>Nạp dữ liệu Hành chính công vào Lakehouse</h2>
            
//             {/* Vùng Kéo Thả */}
//             <div 
//                 onDragOver={onDragOver}
//                 onDragLeave={onDragLeave}
//                 onDrop={onDrop}
//                 style={{
//                     border: isDragging ? '2px dashed #4CAF50' : '2px dashed #ccc',
//                     backgroundColor: isDragging ? '#e8f5e9' : '#fafafa',
//                     padding: '60px',
//                     textAlign: 'center',
//                     borderRadius: '10px',
//                     cursor: 'pointer',
//                     transition: '0.3s'
//                 }}
//             >
//                 <p style={{ fontSize: '18px', color: '#555' }}>
//                     {isDragging ? "Thả file của bạn vào đây..." : "Kéo và thả file Báo cáo (PDF, Word) vào đây"}
//                 </p>
//                 <p style={{ fontSize: '12px', color: '#999' }}>*Dữ liệu sẽ được đẩy thẳng vào vùng Bronze của MinIO</p>
//             </div>

//             {/* Thông báo trạng thái */}
//             {uploadStatus && (
//                 <div style={{ marginTop: '20px', padding: '10px', backgroundColor: '#e3f2fd', borderRadius: '5px' }}>
//                     <strong>Trạng thái: </strong> {uploadStatus}
//                 </div>
//             )}
//         </div>
//     );
// };

// export default UploadComponent;