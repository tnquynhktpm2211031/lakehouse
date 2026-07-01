from kafka import KafkaProducer
import json
from datetime import datetime

# Kết nối vào Kafka ở cổng 9094 (cổng ra bên ngoài của Docker)
producer = KafkaProducer(
    bootstrap_servers=['localhost:9094'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Tạo một tin nhắn mồi giả lập chỉ tiêu chất lượng CUSC
dummy_data = {
    "ma_chi_tieu": "QTCL-MT01",
    "nhom_don_vi": "QTCL",
    "quy_danh_gia": "Q1/2026",
    "ket_qua_he_thong": "ĐẠT",
    "thoi_gian_cap_nhat": datetime.now().isoformat()
}

# Gửi tin nhắn mồi vào topic mới
print("⏳ Đang gửi tin nhắn mồi để khởi tạo Topic...")
producer.send("cusc_kpi_events", dummy_data)
producer.flush()

print("✅ Đã tạo topic 'cusc_kpi_events' và gửi tin nhắn thành công!")