import sys
import os
from env_config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, NESSIE_API_URL, HADOOP_HOME, SPARK_LOCAL_IP
from pyspark.sql import SparkSession

os.environ["HADOOP_HOME"] = HADOOP_HOME
os.environ["PATH"] = os.path.join(HADOOP_HOME, "bin") + ";" + os.environ.get("PATH", "")
os.environ["AWS_ACCESS_KEY_ID"] = MINIO_ACCESS_KEY
os.environ["AWS_SECRET_ACCESS_KEY"] = MINIO_SECRET_KEY
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,"
    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,"
    "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 "
    "pyspark-shell"
)

spark = SparkSession.builder \
    .appName("TestNessie") \
    .config("spark.driver.host", SPARK_LOCAL_IP) \
    .config("spark.driver.bindAddress", SPARK_LOCAL_IP) \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions") \
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lakehouse.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config("spark.sql.catalog.lakehouse.uri", NESSIE_API_URL) \
    .config("spark.sql.catalog.lakehouse.warehouse", "s3a://university-lakehouse/iceberg-warehouse") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

print("Spark initialized")
spark.sql("CREATE BRANCH IF NOT EXISTS test_branch IN lakehouse FROM main")
spark.sql("USE REFERENCE test_branch IN lakehouse")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.test_db")
spark.sql("CREATE TABLE IF NOT EXISTS lakehouse.test_db.test_table (id INT) USING iceberg")
spark.sql("INSERT INTO lakehouse.test_db.test_table VALUES (1)")
spark.sql("MERGE BRANCH test_branch INTO main IN lakehouse")
print("Merge successful!")
