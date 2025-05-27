#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Demo kết nối PySpark với PostgreSQL - Phiên bản đơn giản
"""

from pyspark.sql import SparkSession
import time

def main():
    print("🏦 === HDBANK PYSPARK POSTGRESQL JDBC DEMO ===")
    
    # Khởi tạo SparkSession
    print("🚀 Đang khởi tạo SparkSession...")
    spark = SparkSession.builder \
        .appName("HDBank_PostgreSQL_Simple") \
        .config("spark.jars", "/home/devduongthanhdat/Desktop/hdbank/hdbank_pyspark/postgresql-42.7.2.jar") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    print("✅ SparkSession đã được khởi tạo!")
    
    # Cấu hình JDBC
    jdbc_url = "jdbc:postgresql://localhost:5432/hdbank_db"
    connection_properties = {
        "user": "postgres",
        "password": "hdbank123",
        "driver": "org.postgresql.Driver"
    }
    
    try:
        print("📊 Tạo dữ liệu mẫu...")
        # Tạo DataFrame đơn giản
        data = [
            (1, "Nguyễn Văn An", 25, 15000000.0),
            (2, "Trần Thị Bình", 30, 25000000.0),
            (3, "Lê Văn Cường", 28, 8000000.0)
        ]
        columns = ["id", "name", "age", "balance"]
        df = spark.createDataFrame(data, columns)
        
        print("📝 Ghi vào PostgreSQL...")
        df.write \
            .mode("overwrite") \
            .jdbc(jdbc_url, "test_customers", properties=connection_properties)
        print("✅ Ghi thành công!")
        
        print("📖 Đọc từ PostgreSQL...")
        df_read = spark.read \
            .jdbc(jdbc_url, "test_customers", properties=connection_properties)
        
        print("📊 Kết quả:")
        df_read.show()
        
        print("🎉 Demo thành công!")
        
    except Exception as e:
        print(f"❌ Lỗi: {str(e)}")
        
    finally:
        spark.stop()
        print("✅ Đã đóng SparkSession")

if __name__ == "__main__":
    main()
