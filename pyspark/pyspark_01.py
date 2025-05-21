# Import các thư viện cần thiết
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Tạo SparkSession (điểm khởi đầu của PySpark)
spark = SparkSession.builder \
    .appName("SimplePySparkExample") \
    .getOrCreate()

# Tạo dữ liệu mẫu: danh sách khách hàng (tên, tuổi, doanh thu)
data = [
    ("An", 25, 5000),
    ("Bình", 30, 7000),
    ("Cường", 22, 3000),
    ("Dung", 28, 6000),
    ("Hà", 35, 8000)
]

# Tạo DataFrame từ dữ liệu
df = spark.createDataFrame(data, ["name", "age", "revenue"])

# Hiển thị DataFrame ban đầu
print("Dữ liệu ban đầu:")
df.show()

# Hiển thị cấu trúc của DataFrame
df.printSchema()

df_filter = df.filter(df.age > 30)
df_filter.show()

# Sắp xếp theo tuổi giảm dần
df_sorted = df.orderBy(df.age)
df_sorted.show()

# Đếm số người theo tuổi
df_grouped = df.groupBy("age").count()
df_grouped.show()

# Đọc file CSV
df_csv = spark.read.csv("data.csv", header=True, inferSchema=True)
df_csv.show()

# Ghi DataFrame ra file JSON
df_csv.write.json("output_people.json")