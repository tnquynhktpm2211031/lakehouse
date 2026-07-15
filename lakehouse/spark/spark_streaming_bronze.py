# """
# spark_streaming_bronze.py
# ------------------------------------------------------------
# Đọc luồng sự kiện KPI CUSC từ Kafka (topic: cusc_kpi_events),
# parse JSON theo schema chuẩn, rồi ghi liên tục (streaming append)
# vào bronze/cusc_kpi_stream/ trên MinIO dưới dạng Parquet.

# Chạy: python spark_streaming_bronze.py
# Dừng: Ctrl+C (checkpoint đã lưu, chạy lại sẽ tiếp tục từ vị trí cũ)
# ------------------------------------------------------------
# """

# import os
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, from_json
# from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# from env_config import MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_ENDPOINT, HADOOP_HOME, SPARK_LOCAL_IP

# # ============================================================
# # CAU HINH MOI TRUONG WINDOWS & HADOOP
# # ============================================================
# os.environ["HADOOP_HOME"]  = HADOOP_HOME
# os.environ["PATH"]         = os.path.join(HADOOP_HOME, "bin") + ";" + os.environ.get("PATH", "")
# os.environ["SPARK_LOCAL_IP"] = SPARK_LOCAL_IP
# os.environ["PYSPARK_SUBMIT_ARGS"] = (
#     "--packages "
#     "org.apache.hadoop:hadoop-aws:3.3.4,"
#     "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
#     "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
#     "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.104.5,"
#     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 "
#     "pyspark-shell"
# )

# KAFKA_BOOTSTRAP_SERVERS = "localhost:9094"
# KAFKA_TOPIC = "cusc_kpi_events"

# BRONZE_STREAM_PATH = "s3a://university-lakehouse/bronze/cusc_kpi_stream/"
# CHECKPOINT_PATH = "s3a://university-lakehouse/checkpoints/bronze_cusc_kpi_stream/"

# # Schema khớp với dữ liệu do mock_cusc_kpi.py gửi lên Kafka
# SCHEMA_CUSC = StructType(
#     [
#         StructField("ma_chi_tieu", StringType(), True),
#         StructField("nhom_don_vi", StringType(), True),  # QTCL, RD, HT, VP...
#         StructField("quy_danh_gia", StringType(), True),
#         StructField("ket_qua_he_thong", StringType(), True),
#         StructField("thoi_gian_cap_nhat", TimestampType(), True),
#     ]
# )


# def build_spark_session() -> SparkSession:
#     return (
#         SparkSession.builder.appName("CUSC-Streaming-To-Bronze")
#         .master("local[*]")
#         .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
#         .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
#         .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
#         .config("spark.hadoop.fs.s3a.path.style.access", "true")
#         .config("spark.hadoop.io.nativeio.NativeIO", "false")
#         .getOrCreate()
#     )


# def main():
#     spark = build_spark_session()
#     spark.sparkContext.setLogLevel("WARN")

#     df_kafka = (
#         spark.readStream.format("kafka")
#         .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
#         .option("subscribe", KAFKA_TOPIC)
#         .option("startingOffsets", "latest")
#         .load()
#     )

#     df_parsed = (
#         df_kafka.selectExpr("CAST(value AS STRING) as json_data")
#         .select(from_json(col("json_data"), SCHEMA_CUSC).alias("data"))
#         .select("data.*")
#     )

#     query = (
#         df_parsed.writeStream.format("parquet")
#         .outputMode("append")
#         .option("path", BRONZE_STREAM_PATH)
#         .option("checkpointLocation", CHECKPOINT_PATH)
#         .start()
#     )

#     print("=== STREAM KPI CUSC ĐÃ KHỞI ĐỘNG ===")
#     print(f"    Kafka topic     : {KAFKA_TOPIC}")
#     print(f"    Ghi dữ liệu vào : {BRONZE_STREAM_PATH}")
#     query.awaitTermination()


# if __name__ == "__main__":
#     main()