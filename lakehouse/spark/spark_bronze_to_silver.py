import os
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, udf, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# =========================================================================
# CẤU HÌNH MÔI TRƯỜNG WINDOWS & HADOOP
# =========================================================================
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")

# 💡 FIX LỖI BLOCKMANAGER / NULLPOINTEREXCEPTION TRÊN WINDOWS
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

spark = (
    SparkSession.builder
    .appName("Gov-Bronze-To-Silver-ETL")
    .master("local[*]")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.104.5")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
    .config("spark.sql.catalog.lakehouse.uri", "http://localhost:19120/api/v1")
    .config("spark.sql.catalog.lakehouse.ref", "main")
    .config("spark.sql.catalog.lakehouse.warehouse", "s3a://university-lakehouse/iceberg-warehouse")
    .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.io.nativeio.NativeIO", "false") # Thêm cấu hình chống lỗi Windows
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# =========================================================================
# 1. KHỞI TẠO BẢNG ICEBERG (TẦNG SILVER)
# =========================================================================
spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.silver.ho_so_tich_hop
(
    ma_ho_so STRING,
    nguon_du_lieu STRING,
    ma_cong_dan STRING,
    loai_tai_lieu STRING,
    trang_thai_ho_so STRING,
    trang_thai_chu_ky_so STRING,
    diem_thi_danh_gia DOUBLE,
    thoi_gian_tiep_nhan TIMESTAMP
)
USING ICEBERG
""")

# =========================================================================
# 2. HÀM NLP / REGEX BÓC TÁCH DỮ LIỆU PHI CẤU TRÚC (UDF)
# =========================================================================
nlp_schema = StructType([
    StructField("ma_ho_so_ext", StringType(), True),
    StructField("diem_thi_ext", DoubleType(), True)
])

def extract_info_from_text(noi_dung_van_ban):
    if not noi_dung_van_ban:
        return (None, None)
    
    ma_match = re.search(r"Mã HS:\s*([A-Z0-9_]+)", str(noi_dung_van_ban))
    diem_match = re.search(r"Điểm:\s*([\d\.]+)", str(noi_dung_van_ban))
    
    ma_hs = ma_match.group(1) if ma_match else None
    diem = float(diem_match.group(1)) if diem_match else None
    
    return (ma_hs, diem)

extract_udf = udf(extract_info_from_text, nlp_schema)

# =========================================================================
# 3. ĐỌC DỮ LIỆU TỪ TẦNG BRONZE VÀ LÀM SẠCH (DATA CLEANSING & MAPPING)
# =========================================================================

# 💡 LOẠI BỎ THƯ MỤC CŨ: Chỉ đọc từ thư mục chuẩn của luồng hệ thống
bronze_paths = [
        "s3a://university-lakehouse/bronze/structured_data/",
        "s3a://university-lakehouse/bronze/unstructured_data/"
    ]

# BÍ QUYẾT FIX LỖI WRONG FS S3A: Lấy đúng hệ thống file của đường dẫn
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
for bronze_path in bronze_paths:
    path = spark._jvm.org.apache.hadoop.fs.Path(bronze_path)
    fs = path.getFileSystem(hadoop_conf)

if not fs.exists(path):
    fs.mkdirs(path)
    print(f"[*] Đã tự động tạo thư mục rỗng trên MinIO tại: {bronze_path}")

# Khai báo Schema bắt buộc cho luồng Streaming
schema_bronze = StructType([
    StructField("ma_ho_so", StringType(), True),
    StructField("nguon_du_lieu", StringType(), True),
    StructField("ma_cong_dan", StringType(), True),
    StructField("loai_tai_lieu", StringType(), True),
    StructField("trang_thai_ho_so", StringType(), True),
    StructField("trang_thai_chu_ky_so", StringType(), True),
    StructField("diem_thi_danh_gia", DoubleType(), True),
    StructField("thoi_gian_tiep_nhan", TimestampType(), True),
    StructField("noi_dung_van_ban", StringType(), True)
])

# Đọc luồng dữ liệu thô
df_bronze_stream = spark.readStream.schema(schema_bronze).parquet(bronze_paths[0])

# Thực hiện ETL
df_silver_stream = (
    df_bronze_stream
    .withColumn("ma_cong_dan", trim(col("ma_cong_dan")))
    .withColumn("trang_thai_chu_ky_so", lower(trim(col("trang_thai_chu_ky_so"))))
    .filter(col("ma_cong_dan").isNotNull())
    .filter(col("ma_ho_so").isNotNull() | col("noi_dung_van_ban").isNotNull())
)

# =========================================================================
# 4. GHI VÀO ICEBERG BẰNG CƠ CHẾ ACID
# =========================================================================
query = (
    df_silver_stream
    .select("ma_ho_so", "nguon_du_lieu", "ma_cong_dan", "loai_tai_lieu", 
            "trang_thai_ho_so", "trang_thai_chu_ky_so", "diem_thi_danh_gia", "thoi_gian_tiep_nhan")
    .writeStream
    .format("iceberg")
    .outputMode("append")
    .trigger(processingTime="1 minute")
    # 💡 TẠO CHECKPOINT MỚI TINH ĐỂ TRÁNH XUNG ĐỘT
    .option("checkpointLocation", "s3a://university-lakehouse/checkpoints/silver/ho_so_structured_etl_new")
    .toTable("lakehouse.silver.ho_so_tich_hop")
)

print("\n[*] Quá trình Streaming Bronze -> Silver (ETL + NLP + Iceberg) đang chạy...")
print(f"[*] Nguồn đang lắng nghe: {bronze_path}")
query.awaitTermination()