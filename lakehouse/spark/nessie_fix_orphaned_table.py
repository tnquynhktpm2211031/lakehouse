# -*- coding: utf-8 -*-
"""
nessie_fix_orphaned_table.py
------------------------------------------------------------
Sửa lỗi: Nessie catalog (Postgres) còn tham chiếu tới 1 bảng Iceberg
mà file metadata.json vật lý trên MinIO đã KHÔNG còn tồn tại (thường
xảy ra khi dev xoá volume/dữ liệu MinIO nhưng KHÔNG đồng thời reset
Postgres của Nessie -> 2 nơi lưu trạng thái bị lệch nhau).

Vì Spark/Iceberg khi CREATE/DROP TABLE đều cố "doRefresh" (đọc
metadata.json hiện tại) trước khi làm bất cứ điều gì, script này
KHÔNG dùng Spark mà gọi THẲNG Nessie REST API để xoá tham chiếu
(content key) khỏi nhánh 'main' -> không cần mở file nên không bị lỗi
NotFoundException.

SAU KHI CHẠY XONG SCRIPT NÀY: chạy lại pipeline bình thường
(spark_bronze_to_silver.py / spark_silver_to_gold.py), bảng sẽ được
Spark tự tạo mới hoàn toàn sạch (CREATE TABLE IF NOT EXISTS).

Cách dùng:
    pip install requests   (nếu máy chưa có)

    python nessie_fix_orphaned_table.py silver.kpi_cusc_master
    python nessie_fix_orphaned_table.py gold.kpi_tong_hop_don_vi gold.kpi_chi_tiet_dashboard
------------------------------------------------------------
"""

import sys
import requests

# Dùng API v1 vì đó là giao thức mà nessie-spark-extensions 0.77.1 (phía pipeline)
# thực sự nói được -> giữ nhất quán, tránh 2 phía client nói lệch phiên bản.
NESSIE_API_URL = "http://localhost:19120/api/v1"
BRANCH = "main"


def get_current_hash() -> str:
    resp = requests.get(f"{NESSIE_API_URL}/trees/tree/{BRANCH}")
    resp.raise_for_status()
    return resp.json()["hash"]


def delete_content_key(table_path: str, expected_hash: str) -> bool:
    """table_path dạng 'silver.kpi_cusc_master' -> content key ['silver', 'kpi_cusc_master']
    (LƯU Ý: không có tiền tố 'lakehouse' vì đó chỉ là tên catalog phía Spark,
    Nessie chỉ lưu namespace + tên bảng)."""
    elements = table_path.split(".")
    body = {
        "commitMeta": {"message": f"Repair: xoá tham chiếu orphaned '{table_path}'"},
        "operations": [
            {"type": "DELETE", "key": {"elements": elements}}
        ],
    }
    resp = requests.post(
        f"{NESSIE_API_URL}/trees/branch/{BRANCH}/commit",
        params={"expectedHash": expected_hash},
        json=body,
    )
    if resp.status_code >= 400:
        print(f"❌ Lỗi khi xoá '{table_path}': {resp.status_code} {resp.text}")
        return False
    print(f"✅ Đã xoá tham chiếu orphaned '{table_path}' khỏi Nessie catalog (branch main).")
    return True


def main():
    if len(sys.argv) < 2:
        print("Cách dùng: python nessie_fix_orphaned_table.py <namespace.table> [<namespace.table> ...]")
        print("Ví dụ:     python nessie_fix_orphaned_table.py silver.kpi_cusc_master")
        sys.exit(1)

    for table_path in sys.argv[1:]:
        # Lấy hash mới nhất TRƯỚC MỖI lần commit (hash thay đổi sau mỗi thao tác xoá)
        current_hash = get_current_hash()
        delete_content_key(table_path, current_hash)


if __name__ == "__main__":
    main()