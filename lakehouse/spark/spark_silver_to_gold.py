import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, round, avg

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.104.5 pyspark-shell"

spark = (
    SparkSession.builder
    .appName("Gov-Silver-To-Gold-KPI")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
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
    .config("spark.hadoop.io.nativeio.NativeIO", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

try:
    print("--- Đọc dữ liệu sạch từ bảng Silver ---")
    df_ho_so = spark.read.table("lakehouse.silver.ho_so_tich_hop")

    print("\n--- Tổng hợp KPI 1: Tình trạng xử lý Hành chính công theo Nguồn ---")
    df_kpi_nguon = (
        df_ho_so
        .groupBy("nguon_du_lieu")
        .agg(
            count("ma_ho_so").alias("tong_so_ho_so"),
            count(when(col("trang_thai_chu_ky_so") == "hople", 1)).alias("so_chu_ky_hop_le"),
            count(when(col("trang_thai_chu_ky_so") == "khonghople", 1)).alias("so_chu_ky_loi")
        )
        .withColumn("ty_le_chu_ky_hop_le", round(col("so_chu_ky_hop_le") * 100.0 / col("tong_so_ho_so"), 2))
    )

    print("\n--- Tổng hợp KPI 2: Thống kê Điểm thi Bộ GDĐT ---")
    df_kpi_gddt = (
        df_ho_so
        .filter(col("nguon_du_lieu") == "Bo_GDDT")
        .groupBy("loai_tai_lieu")
        .agg(
            round(avg("diem_thi_danh_gia"), 2).alias("diem_trung_binh"),
            count("ma_ho_so").alias("tong_so_bai_thi")
        )
    )

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

    print("Ghi Data Mart: lakehouse.gold.kpi_hanh_chinh")
    df_kpi_nguon.writeTo("lakehouse.gold.kpi_hanh_chinh").createOrReplace()

    print("Ghi Data Mart: lakehouse.gold.kpi_giao_duc")
    df_kpi_gddt.writeTo("lakehouse.gold.kpi_giao_duc").createOrReplace()

    print("\n✔️ KẾT QUẢ: PIPELINE GOLD THÀNH CÔNG (SUCCESS)")
    spark.read.table("lakehouse.gold.kpi_hanh_chinh").show(truncate=False)

except Exception as ex:
    print("\n❌ LỖI HỆ THỐNG:", str(ex))
finally:
    spark.stop()