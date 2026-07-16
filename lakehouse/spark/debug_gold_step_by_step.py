# import sys
# print("1. Bắt đầu file", flush=True)

# import os
# print("2. Đã import os", flush=True)

# from pyspark.sql import SparkSession
# print("3. Đã import SparkSession", flush=True)

# from pyspark.sql.functions import col, when, count, round, current_timestamp, lag, create_map, lit, regexp_extract
# print("4. Đã import pyspark.sql.functions", flush=True)

# from pyspark.sql.window import Window
# print("5. Đã import Window", flush=True)

# from itertools import chain
# print("6. Đã import itertools.chain", flush=True)

# from nessie_catalog_utils import (
#     make_branch_name, create_branch, use_branch, use_main,
#     merge_branch_to_main, check_quality_gold, DataQualityError,
# )
# print("7. Đã import nessie_catalog_utils", flush=True)

# from openmetadata_lineage_utils import get_client, push_lineage_safe
# print("8. Đã import openmetadata_lineage_utils", flush=True)

# from env_config import (
#     MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_ENDPOINT,
#     NESSIE_API_URL, HADOOP_HOME, SPARK_LOCAL_IP,
# )
# print("9. Đã import env_config", flush=True)

# os.environ["HADOOP_HOME"] = HADOOP_HOME
# os.environ["PATH"] = os.path.join(HADOOP_HOME, "bin") + ";" + os.environ.get("PATH", "")
# os.environ["AWS_ACCESS_KEY_ID"] = MINIO_ACCESS_KEY
# os.environ["AWS_SECRET_ACCESS_KEY"] = MINIO_SECRET_KEY
# os.environ["SPARK_LOCAL_IP"] = SPARK_LOCAL_IP
# os.environ["PYSPARK_SUBMIT_ARGS"] = (
#     "--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,"
#     "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,"
#     "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 "
#     "pyspark-shell"
# )
# print("10. Đã set biến môi trường", flush=True)

# print("11. Chuẩn bị gọi SparkSession.builder...", flush=True)
# spark = (
#     SparkSession.builder
#     .appName("Debug_Gold")
#     .config("spark.driver.host", SPARK_LOCAL_IP)
#     .config("spark.driver.bindAddress", SPARK_LOCAL_IP)
#     .config("spark.sql.extensions",
#             "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
#             "org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
#     .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
#     .config("spark.sql.catalog.lakehouse.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
#     .config("spark.sql.catalog.lakehouse.uri", NESSIE_API_URL)
#     .config("spark.sql.catalog.lakehouse.warehouse", "s3a://university-lakehouse/iceberg-warehouse")
#     .config("spark.sql.catalog.lakehouse.s3.endpoint", MINIO_ENDPOINT)
#     .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
#     .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
#     .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
#     .config("spark.hadoop.fs.s3a.path.style.access", "true")
#     .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
#     .config("spark.hadoop.fs.s3a.aws.credentials.provider",
#             "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
#     .getOrCreate()
# )
# print("12. Đã tạo Spark Session THÀNH CÔNG!", flush=True)

# df = spark.read.table("lakehouse.silver.kpi_cusc_master")
# print(f"13. Đọc bảng Silver OK, số dòng = {df.count()}", flush=True)

# spark.stop()
# print("14. HOÀN TẤT DEBUG", flush=True)