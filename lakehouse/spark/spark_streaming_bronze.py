import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json 
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# 🛠️ KHIÊN CHỐNG LỖI WINDOWS & THƯ VIỆN ĐẦY ĐỦ
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.104.5,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell"

spark = (
    SparkSession.builder
    .appName("Gov-Streaming-To-Bronze")
    .master("local[*]")
    #.config("spark.driver.host", "127.0.0.1")
    #.config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.io.nativeio.NativeIO", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema_tich_hop = StructType([
    StructField("ma_ho_so", StringType(), True),
    StructField("nguon_du_lieu", StringType(), True),
    StructField("ma_cong_dan", StringType(), True),
    StructField("loai_tai_lieu", StringType(), True),
    StructField("trang_thai_ho_so", StringType(), True), 
    StructField("trang_thai_chu_ky_so", StringType(), True),
    StructField("diem_thi_danh_gia", DoubleType(), True),
    StructField("thoi_gian_tiep_nhan", TimestampType(), True)
])

df_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9094")
    .option("subscribe", "gov_admin_events")
    .option("startingOffsets", "latest")
    .load()
)

df_parsed = (
    df_kafka
    .selectExpr("CAST(value AS STRING) as json_data")
    .select(from_json(col("json_data"), schema_tich_hop).alias("data"))
    .select("data.*")
)

# Ghi vào vùng Bronze dành riêng cho Streaming
bronze_stream_path = "s3a://university-lakehouse/bronze/ho_so_stream/"
checkpoint_path = "s3a://university-lakehouse/checkpoints/bronze_ho_so_stream/"

query = (
    df_parsed.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", bronze_stream_path)
    .option("checkpointLocation", checkpoint_path)
    .start()
)

print("=== STREAM TỪ TRỤC LIÊN THÔNG ĐÃ KHỞI ĐỘNG ===")
query.awaitTermination()