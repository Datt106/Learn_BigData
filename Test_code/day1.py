import os
import sys

# Ép Spark sử dụng đúng phiên bản Python đang chạy file này
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
from pyspark.sql import SparkSession

# Bước 1: Khởi tạo SparkSession
# Đây là điểm neo kết nối code của bạn với engine Spark và Hadoop (nếu cần)
spark = SparkSession.builder \
    .appName("Mini_ETL_Pipeline") \
    .master("local[*]") \
    .getOrCreate()

# Giảm bớt log rác (WARN/INFO) hiển thị trên terminal để dễ nhìn kết quả
spark.sparkContext.setLogLevel("ERROR")

print("--- Đã khởi động SparkSession thành công! ---")

# Bước 2: Extract - Tạo dữ liệu mẫu 
# (Thực tế bạn sẽ đọc từ HDFS, database hoặc Data Warehouse)
# ... (Phần khởi tạo SparkSession ở trên giữ nguyên) ...

# Bước 2 (MỚI): Đọc dữ liệu từ file CSV
# - header=True: Báo cho Spark biết dòng đầu tiên là tên các cột
# - inferSchema=True: Báo cho Spark tự động nhận diện đâu là chữ, đâu là số
duong_dan_file = "Test_code/nhan_vien.csv" 
df_nhan_vien = spark.read.csv(duong_dan_file, header=True, inferSchema=True)

print("\nDữ liệu gốc đọc từ file CSV:")
df_nhan_vien.show()

# ... (Phần Transform bằng SQL và Load ghi ra file ở dưới giữ nguyên) ...

df_nhan_vien = spark.createDataFrame(data, columns)
print("\nDữ liệu gốc:")
df_nhan_vien.show()

# Bước 3: Transform - Biến đổi dữ liệu bằng Spark SQL
# Chuyển DataFrame thành một bảng tạm (Temporary View) để dùng SQL thuần
df_nhan_vien.createOrReplaceTempView("bang_nhan_vien")

# Truy vấn: Tìm nhân viên IT có lương >= 1500, sắp xếp lương giảm dần
df_it_luong_cao = spark.sql("""
    SELECT Ten, Luong
    FROM bang_nhan_vien
    WHERE PhongBan = 'IT' AND Luong >= 1500
    ORDER BY Luong DESC
""")

print("\nKết quả sau khi Transform (Nhân viên IT lương >= 1500):")
df_it_luong_cao.show()

# Bước 4: Load - Ghi kết quả ra file
# Ở chế độ local, nó sẽ tạo một thư mục chứa các file csv/parquet đã phân mảnh
output_path = "output_nhan_vien"
df_it_luong_cao.write.mode("overwrite").csv(output_path, header=True)
print(f"\nĐã ghi kết quả ra thư mục: {output_path}")

# Đóng ứng dụng để giải phóng RAM/CPU
spark.stop()