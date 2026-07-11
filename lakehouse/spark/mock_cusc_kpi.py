"""
mock_cusc_kpi.py
------------------------------------------------------------
Gửi 1 (hoặc nhiều) tin nhắn giả lập chỉ tiêu chất lượng CUSC vào
topic Kafka "cusc_kpi_events", dùng để khởi tạo topic và test
luồng streaming (spark_streaming_bronze.py).

Chạy: python mock_cusc_kpi.py
------------------------------------------------------------
"""

import json
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = ["localhost:9094"]
KAFKA_TOPIC = "cusc_kpi_events"

SAMPLE_RECORDS = [
    {
        "ma_chi_tieu": "QTCL-MT01",
        "nhom_don_vi": "QTCL",
        "quy_danh_gia": "Q1/2026",
        "ket_qua_he_thong": "ĐẠT",
    },
    {
        "ma_chi_tieu": "RD-KPI02",
        "nhom_don_vi": "RD",
        "quy_danh_gia": "Q1/2026",
        "ket_qua_he_thong": "KHÔNG ĐẠT",
    },
    {
        "ma_chi_tieu": "HT-KPI03",
        "nhom_don_vi": "HT",
        "quy_danh_gia": "Q1/2026",
        "ket_qua_he_thong": "CHƯA ĐẾN KỲ ĐÁNH GIÁ",
    },
]


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )

    print(f"⏳ Đang gửi {len(SAMPLE_RECORDS)} tin nhắn mồi vào topic '{KAFKA_TOPIC}'...")
    for record in SAMPLE_RECORDS:
        record["thoi_gian_cap_nhat"] = datetime.now().isoformat()
        producer.send(KAFKA_TOPIC, record)
        print(f"   → Đã gửi: {record['ma_chi_tieu']} ({record['ket_qua_he_thong']})")

    producer.flush()
    producer.close()
    print(f"✅ Đã tạo topic '{KAFKA_TOPIC}' và gửi tin nhắn thành công!")


if __name__ == "__main__":
    main()