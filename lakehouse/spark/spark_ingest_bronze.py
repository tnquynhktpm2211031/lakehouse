import os
import glob
from pyspark.sql import SparkSession

# 🛠️ CẤU HÌNH BIẾN MÔI TRƯỜNG CHO WINDOWS
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 pyspark-shell"

print("\n=========================================================")
print("🚀 SCRIPT NẠP DỮ LIỆU ĐA ĐỊNH DẠNG VÀO TẦNG BRONZE")
print("=========================================================\n")

spark = (
    SparkSession.builder 
    .appName("Gov-Omni-Ingestion-Bronze") 
    .master("local[*]") 
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000") 
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") 
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") 
    .config("spark.hadoop.fs.s3a.path.style.access", "true") 
    .config("spark.hadoop.io.nativeio.NativeIO", "false") 
    .getOrCreate()
)

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.timeout", "60000")

try:
    # --- ĐOẠN SỬA ĐỔI BẮT ĐẦU TỪ ĐÂY ---
    data_dir = r"D:\Myfolder\University\ThucTap\university-lakehouse\data_local"
    
    if not os.path.exists(data_dir):
        print(f"--- Thư mục {data_dir} không tồn tại ---")
    else:
        # Dùng os.listdir thay cho glob để quét sạch mọi file
        files_in_dir = os.listdir(data_dir)
        
        if not files_in_dir:
            print("--- Không tìm thấy file dữ liệu nào trong thư mục data_local ---")
        else:
            for file_name in files_in_dir:
                file_path = os.path.join(data_dir, file_name)
                
                # Bỏ qua nếu là thư mục con, chỉ xử lý file
                if os.path.isfile(file_path):
                    # Lấy đuôi file và chuyển hết về chữ thường để không bị miss file .PNG hay .JPG
                    file_ext = file_name.split('.')[-1].lower() 
                    
                    print(f"\n--- Đang xử lý file: {file_name} ---")
                    
                    # 1. DỮ LIỆU CÓ CẤU TRÚC (CSV)
                    if file_ext == 'csv':
                        df = spark.read.csv(file_path, header=True, inferSchema=True)
                        bronze_path = "s3a://university-lakehouse/bronze/structured_data/"
                        df.write.mode("append").parquet(bronze_path)
                        print(f"✅ Ghi thành công CSV dạng Parquet vào: {bronze_path}")

                    # 2. DỮ LIỆU BÁN CẤU TRÚC (JSON)
                    elif file_ext == 'json':
                        df = spark.read.json(file_path)
                        bronze_path = "s3a://university-lakehouse/bronze/semi_structured_data/"
                        df.write.mode("append").parquet(bronze_path)
                        print(f"✅ Ghi thành công JSON dạng Parquet vào: {bronze_path}")

                    # 3. DỮ LIỆU PHI CẤU TRÚC (PDF, DOCX, TXT, Hình ảnh)
                    elif file_ext in ['pdf', 'doc', 'docx', 'txt', 'png', 'jpg', 'jpeg']:
                        # Dùng binaryFile để lưu thô định dạng byte (nguyên bản)
                        df_binary = spark.read.format("binaryFile").load(file_path)
                        bronze_path = "s3a://university-lakehouse/bronze/unstructured_data/"
                        df_binary.write.mode("append").parquet(bronze_path)
                        print(f"✅ Ghi thành công Phi cấu trúc dạng Parquet vào: {bronze_path}")

                    else:
                        print(f"⚠️ Bỏ qua định dạng chưa hỗ trợ: {file_name}")

except Exception as e:
    print(f"\n❌ LỖI TRONG QUÁ TRÌNH NẠP DỮ LIỆU: {str(e)}")

finally:
    print("\n--- Đang đóng tiến trình Spark Session ---")
    spark.sparkContext.setLogLevel("FATAL")
    spark.stop()