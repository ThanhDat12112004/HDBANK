# -*- coding: utf-8 -*-
"""
Kết nối PySpark với PostgreSQL thông qua JDBC
Tác giả: HDBank Data Team
Ngày: 27/05/2025
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import time

def create_spark_session():
    """Tạo SparkSession với JDBC driver cho PostgreSQL"""
    print("🚀 Đang khởi tạo SparkSession với PostgreSQL JDBC...")
    
    spark = SparkSession.builder \
        .appName("HDBank_PostgreSQL_JDBC") \
        .config("spark.jars", "/home/devduongthanhdat/Desktop/hdbank/hdbank_pyspark/postgresql-42.7.2.jar") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    print("✅ SparkSession đã được khởi tạo thành công!")
    return spark

def create_jdbc_connection_properties():
    """Tạo thông tin kết nối JDBC"""
    return {
        "url": "jdbc:postgresql://localhost:5432/hdbank_db",
        "properties": {
            "user": "postgres",
            "password": "hdbank123",
            "driver": "org.postgresql.Driver"
        }
    }

def create_sample_data(spark):
    """Tạo dữ liệu mẫu cho demo"""
    print("📊 Đang tạo dữ liệu mẫu...")
    
    # Tạo dữ liệu khách hàng HDBank
    customer_data = [
        (1, "Nguyễn Văn An", "0901234567", "HCM", 25, 15000000.0, "VIP"),
        (2, "Trần Thị Bình", "0907654321", "HN", 30, 25000000.0, "GOLD"),
        (3, "Lê Văn Cường", "0903456789", "DN", 28, 8000000.0, "SILVER"),
        (4, "Phạm Thị Dung", "0905678901", "CT", 35, 50000000.0, "DIAMOND"),
        (5, "Hoàng Văn Em", "0902345678", "HCM", 22, 3000000.0, "BASIC"),
        (6, "Vũ Thị Fương", "0908765432", "HN", 27, 12000000.0, "SILVER"),
        (7, "Đỗ Văn Giang", "0904567890", "HP", 33, 35000000.0, "VIP"),
        (8, "Bùi Thị Hạnh", "0906789012", "HCM", 29, 18000000.0, "GOLD"),
        (9, "Ngô Văn Ích", "0901357924", "DN", 31, 22000000.0, "GOLD"),
        (10, "Trương Thị Kim", "0907531864", "CT", 26, 6000000.0, "SILVER")
    ]
    
    schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("customer_name", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("city", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("balance", DoubleType(), True),
        StructField("tier", StringType(), True)
    ])
    
    df = spark.createDataFrame(customer_data, schema)
    print(f"✅ Đã tạo {df.count()} bản ghi khách hàng")
    return df

def write_to_postgresql(df, table_name, jdbc_config):
    """Ghi DataFrame vào PostgreSQL"""
    print(f"📝 Đang ghi dữ liệu vào bảng '{table_name}'...")
    start_time = time.time()
    
    try:
        df.write \
            .mode("overwrite") \
            .option("driver", jdbc_config["properties"]["driver"]) \
            .jdbc(
                url=jdbc_config["url"],
                table=table_name,
                properties=jdbc_config["properties"]
            )
        
        write_time = time.time() - start_time
        print(f"✅ Ghi dữ liệu thành công! Thời gian: {write_time:.2f} giây")
        
    except Exception as e:
        print(f"❌ Lỗi khi ghi dữ liệu: {str(e)}")

def read_from_postgresql(spark, table_name, jdbc_config):
    """Đọc dữ liệu từ PostgreSQL"""
    print(f"📖 Đang đọc dữ liệu từ bảng '{table_name}'...")
    start_time = time.time()
    
    try:
        df = spark.read \
            .option("driver", jdbc_config["properties"]["driver"]) \
            .jdbc(
                url=jdbc_config["url"],
                table=table_name,
                properties=jdbc_config["properties"]
            )
        
        read_time = time.time() - start_time
        print(f"✅ Đọc dữ liệu thành công! Thời gian: {read_time:.2f} giây")
        return df
        
    except Exception as e:
        print(f"❌ Lỗi khi đọc dữ liệu: {str(e)}")
        return None

def perform_analysis(spark, df):
    """Thực hiện phân tích dữ liệu"""
    print("\n🔍 === PHÂN TÍCH DỮ LIỆU KHÁCH HÀNG HDBANK ===")
    
    # Tạo temp view để thực hiện SQL
    df.createOrReplaceTempView("customers")
    
    print("\n1. 📊 Thống kê tổng quan:")
    df.show()
    
    print("\n2. 💰 Phân tích theo tier khách hàng:")
    tier_analysis = spark.sql("""
        SELECT 
            tier,
            COUNT(*) as so_khach_hang,
            AVG(balance) as so_du_trung_binh,
            AVG(age) as tuoi_trung_binh
        FROM customers 
        GROUP BY tier 
        ORDER BY so_du_trung_binh DESC
    """)
    tier_analysis.show()
    
    print("\n3. 🏙️ Phân tích theo thành phố:")
    city_analysis = spark.sql("""
        SELECT 
            city,
            COUNT(*) as so_khach_hang,
            SUM(balance) as tong_so_du,
            AVG(balance) as so_du_trung_binh
        FROM customers 
        GROUP BY city 
        ORDER BY tong_so_du DESC
    """)
    city_analysis.show()
    
    print("\n4. 🎯 Top 5 khách hàng có số dư cao nhất:")
    top_customers = spark.sql("""
        SELECT customer_name, city, balance, tier
        FROM customers 
        ORDER BY balance DESC 
        LIMIT 5
    """)
    top_customers.show()
    
    print("\n5. 📈 Thống kê theo độ tuổi:")
    age_stats = spark.sql("""
        SELECT 
            CASE 
                WHEN age < 25 THEN 'Dưới 25'
                WHEN age BETWEEN 25 AND 30 THEN '25-30'
                WHEN age BETWEEN 31 AND 35 THEN '31-35'
                ELSE 'Trên 35'
            END as nhom_tuoi,
            COUNT(*) as so_luong,
            AVG(balance) as so_du_trung_binh
        FROM customers 
        GROUP BY 
            CASE 
                WHEN age < 25 THEN 'Dưới 25'
                WHEN age BETWEEN 25 AND 30 THEN '25-30'
                WHEN age BETWEEN 31 AND 35 THEN '31-35'
                ELSE 'Trên 35'
            END
        ORDER BY so_du_trung_binh DESC
    """)
    age_stats.show()

def test_jdbc_performance(spark, jdbc_config):
    """Test hiệu suất JDBC với dữ liệu lớn hơn"""
    print("\n⚡ === TEST HIỆU SUẤT JDBC ===")
    
    # Tạo dữ liệu lớn hơn
    print("📊 Tạo dữ liệu test với 10,000 bản ghi...")
    large_data = []
    for i in range(10000):
        large_data.append((
            i + 1,
            f"Customer_{i+1}",
            f"090{i%9000000:07d}",
            ["HCM", "HN", "DN", "CT", "HP"][i % 5],
            20 + (i % 40),
            1000000.0 + (i * 1000),
            ["BASIC", "SILVER", "GOLD", "VIP", "DIAMOND"][i % 5]
        ))
    
    schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("customer_name", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("city", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("balance", DoubleType(), True),
        StructField("tier", StringType(), True)
    ])
    
    large_df = spark.createDataFrame(large_data, schema)
    
    # Test write performance
    print("\n📝 Test hiệu suất ghi dữ liệu lớn...")
    write_to_postgresql(large_df, "customers_large", jdbc_config)
    
    # Test read performance
    print("\n📖 Test hiệu suất đọc dữ liệu lớn...")
    read_df = read_from_postgresql(spark, "customers_large", jdbc_config)
    if read_df:
        print(f"✅ Đã đọc {read_df.count()} bản ghi")

def main():
    """Hàm chính"""
    print("🏦 === HDBANK PYSPARK POSTGRESQL JDBC DEMO ===\n")
    
    # Khởi tạo Spark
    spark = create_spark_session()
    
    # Cấu hình JDBC
    jdbc_config = create_jdbc_connection_properties()
    
    try:
        # Tạo dữ liệu mẫu
        df = create_sample_data(spark)
        
        # Ghi vào PostgreSQL
        write_to_postgresql(df, "customers", jdbc_config)
        
        # Đọc từ PostgreSQL
        df_from_db = read_from_postgresql(spark, "customers", jdbc_config)
        
        if df_from_db:
            # Thực hiện phân tích
            perform_analysis(spark, df_from_db)
            
            # Test hiệu suất
            test_jdbc_performance(spark, jdbc_config)
        
        print("\n🎉 Demo hoàn thành thành công!")
        
    except Exception as e:
        print(f"❌ Lỗi trong quá trình thực thi: {str(e)}")
        
    finally:
        # Dọn dẹp
        print("\n🧹 Đang dọn dẹp resources...")
        spark.stop()
        print("✅ SparkSession đã được đóng")

if __name__ == "__main__":
    main()