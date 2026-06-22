-- 1. Biểu đồ tròn (Pie Chart): Tỷ trọng Hồ sơ đến từ các Cổng Dịch vụ
SELECT 
    nguon_du_lieu, 
    tong_so_ho_so 
FROM iceberg.gold.kpi_hanh_chinh;

-- 2. Biểu đồ Bar Chart (Gauge): Tỷ lệ hợp lệ của Chữ ký số trên toàn hệ thống
SELECT 
    nguon_du_lieu,
    ty_le_chu_ky_hop_le,
    (100.0 - ty_le_chu_ky_hop_le) AS ty_le_chu_ky_loi
FROM iceberg.gold.kpi_hanh_chinh
ORDER BY ty_le_chu_ky_hop_le DESC;

-- 3. Biểu đồ thẻ (Big Number): Điểm trung bình tổng quan từ dữ liệu Bộ GD&ĐT
SELECT 
    loai_tai_lieu AS mon_thi,
    diem_trung_binh,
    tong_so_bai_thi
FROM iceberg.gold.kpi_giao_duc
ORDER BY diem_trung_binh DESC;