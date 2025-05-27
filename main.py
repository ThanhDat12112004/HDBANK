#Thiết lập môi trường trong Google Colab


from pyspark.sql import SparkSession
import time
import matplotlib.pyplot as plt

# Khởi tạo SparkSession với cấu hình tối ưu
spark = SparkSession.builder \
    .appName("StudentAnalysis") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .getOrCreate()

# Thiết lập log level để giảm cảnh báo
spark.sparkContext.setLogLevel("ERROR")

#Tạo dữ liệu sinh viên và lưu dưới dạng CSV và Parquet
# Tạo dữ liệu mẫu (giảm xuống 100k để tối ưu memory)
print("Đang tạo dữ liệu mẫu...")
num_records = 100_000  # Giảm từ 1M xuống 100k để tối ưu
data = [
    (i, f"Student_{i}", 18 + (i % 10), f"Dept_{i % 5}", round(5.0 + (i % 5) * 0.5, 1))
    for i in range(num_records)
]
columns = ["student_id", "name", "age", "department", "gpa"]

# Tạo DataFrame
print("Đang tạo DataFrame...")
df = spark.createDataFrame(data, columns)
print("DataFrame đã được tạo thành công!")

# Ghi dữ liệu thành file CSV
print("Đang ghi file CSV...")
start_time_csv_write = time.time()
df.write.mode("overwrite").option("header", "true").csv("./content/students.csv")
csv_write_time = time.time() - start_time_csv_write
print(f"Thời gian ghi file CSV: {csv_write_time:.2f} giây")

# Ghi dữ liệu thành file Parquet
print("Đang ghi file Parquet...")
start_time_parquet_write = time.time()
df.write.mode("overwrite").parquet("./content/students.parquet")
parquet_write_time = time.time() - start_time_parquet_write
print(f"Thời gian ghi file Parquet: {parquet_write_time:.2f} giây")

#Đọc dữ liệu và tối ưu hóa
print("\n=== BẮT ĐẦU ĐỌC VÀ PHÂN TÍCH DỮ LIỆU ===")
# Đọc file CSV và tối ưu hóa
start_time_csv_read = time.time()
df_csv = spark.read.option("header", "true").option("inferSchema", "true").csv("./content/students.csv")
df_csv = df_csv.repartition(10).cache()  # Repartition và cache
df_csv.count()  # Action để thực thi cache
csv_read_time = time.time() - start_time_csv_read
print(f"Thời gian đọc và tạo DataFrame từ CSV: {csv_read_time:.2f} giây")

# Đọc file Parquet và tối ưu hóa
start_time_parquet_read = time.time()
df_parquet = spark.read.parquet("./content/students.parquet")
df_parquet = df_parquet.repartition(10).cache()  # Repartition và cache
df_parquet.count()  # Action để thực thi cache
parquet_read_time = time.time() - start_time_parquet_read
print(f"Thời gian đọc và tạo DataFrame từ Parquet: {parquet_read_time:.2f} giây")

# Thực hiện các truy vấn phân tích
df_csv.createOrReplaceTempView("students_csv")
df_parquet.createOrReplaceTempView("students_parquet")
#Lọc sinh viên có GPA > 6.0
# Truy vấn trên CSV
start_time_csv_filter = time.time()
spark.sql("SELECT * FROM students_csv WHERE gpa > 6.0").show(5)
csv_filter_time = time.time() - start_time_csv_filter
print(f"Thời gian lọc trên CSV (GPA > 6.0): {csv_filter_time:.2f} giây")

# Truy vấn trên Parquet
start_time_parquet_filter = time.time()
spark.sql("SELECT * FROM students_parquet WHERE gpa > 6.0").show(5)
parquet_filter_time = time.time() - start_time_parquet_filter
print(f"Thời gian lọc trên Parquet (GPA > 6.0): {parquet_filter_time:.2f} giây")

#Nhóm theo khoa và tính GPA trung bình
# Truy vấn trên CSV
start_time_csv_group = time.time()
df_csv_grouped = spark.sql("SELECT department, AVG(gpa) as avg_gpa FROM students_csv GROUP BY department")
df_csv_grouped.show()
csv_group_time = time.time() - start_time_csv_group
print(f"Thời gian nhóm và tính trung bình trên CSV: {csv_group_time:.2f} giây")

# Truy vấn trên Parquet
start_time_parquet_group = time.time()
df_parquet_grouped = spark.sql("SELECT department, AVG(gpa) as avg_gpa FROM students_parquet GROUP BY department")
df_parquet_grouped.show()
parquet_group_time = time.time() - start_time_parquet_group
print(f"Thời gian nhóm và tính trung bình trên Parquet: {parquet_group_time:.2f} giây")

#Sắp xếp theo GPA và lấy top 10
# Truy vấn trên CSV
start_time_csv_sort = time.time()
spark.sql("SELECT * FROM students_csv ORDER BY gpa DESC LIMIT 10").show()
csv_sort_time = time.time() - start_time_csv_sort
print(f"Thời gian sắp xếp trên CSV: {csv_sort_time:.2f} giây")

# Truy vấn trên Parquet
start_time_parquet_sort = time.time()
spark.sql("SELECT * FROM students_parquet ORDER BY gpa DESC LIMIT 10").show()
parquet_sort_time = time.time() - start_time_parquet_sort
print(f"Thời gian sắp xếp trên Parquet: {parquet_sort_time:.2f} giây")

#Tính tổng thời gian và so sánh
# Tổng thời gian đọc và truy vấn
total_csv_time = csv_read_time + csv_filter_time + csv_group_time + csv_sort_time
total_parquet_time = parquet_read_time + parquet_filter_time + parquet_group_time + parquet_sort_time

print(f"\nTổng thời gian xử lý trên CSV: {total_csv_time:.2f} giây")
print(f"Tổng thời gian xử lý trên Parquet: {total_parquet_time:.2f} giây")

# Đánh giá
print("\nĐánh giá:")
if total_csv_time < total_parquet_time:
    print("CSV nhanh hơn Parquet trong trường hợp này.")
else:
    print("Parquet nhanh hơn CSV trong trường hợp này.")
#Trực quan hóa kết quả
# Dữ liệu thời gian truy vấn
tasks = ["Read", "Filter", "GroupBy", "Sort"]
csv_times = [csv_read_time, csv_filter_time, csv_group_time, csv_sort_time]
parquet_times = [parquet_read_time, parquet_filter_time, parquet_group_time, parquet_sort_time]

# Vẽ biểu đồ
plt.figure(figsize=(10, 6))
plt.plot(tasks, csv_times, marker='o', label='CSV')
plt.plot(tasks, parquet_times, marker='o', label='Parquet')
plt.title("So sánh thời gian xử lý giữa CSV và Parquet")
plt.xlabel("Tác vụ")
plt.ylabel("Thời gian (giây)")
plt.legend()
plt.grid()
plt.savefig('./content/performance_comparison.png', dpi=300, bbox_inches='tight')
print("Biểu đồ đã được lưu tại ./content/performance_comparison.png")

# Dọn dẹp
spark.stop()
print("SparkSession đã được đóng.")