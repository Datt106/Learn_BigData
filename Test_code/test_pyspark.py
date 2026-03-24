# 1. BƯỚC BẮT BUỘC TRÊN LOCAL: Khởi tạo SparkSession
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Day1_Phỏng_Vấn_ICOMM") \
    .config("spark.driver.extraJavaOptions", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED") \
    .config("spark.executor.extraJavaOptions", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED") \
    .getOrCreate()
    
# 2. TẠO DỮ LIỆU ÁO
data = [
    ("Nguyễn Văn A", "IT", 20),
    ("Trần Thị B", "HR", 22),
    ("Lê Văn C", "IT", 25),
    ("Phạm Thị D", "Marketing", 21)
]
columns = ["Name", "Department", "Age"]

df = spark.createDataFrame(data, schema=columns)

# In dữ liệu ra terminal (Trên Databricks dùng display(), ở local dùng .show())
print("--- DỮ LIỆU GỐC ---")
df.show()

# 3. TRANSFORM (Biến đổi dữ liệu)
print("--- 1. KẾT QUẢ SELECT (Chỉ lấy Tên và Tuổi) ---")
df_select = df.select("Name", "Age")
df_select.show()

print("--- 2. KẾT QUẢ FILTER (Lọc người > 21 tuổi) ---")
df_filter = df.filter(df["Age"] > 21)
df_filter.show()

print("--- 3. KẾT QUẢ GROUP BY (Đếm nhân sự theo phòng ban) ---")
df_group = df.groupBy("Department").count()
df_group.show()

# Kết thúc phiên làm việc để giải phóng RAM
spark.stop()