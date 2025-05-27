#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Benchmark PySpark SQL vs DataFrame API - HDBank
==============================================
Script tạo dữ liệu mẫu lớn và so sánh hiệu suất giữa SQL và DataFrame API
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, rand, when, lit, desc, count, sum, current_timestamp
import time
import random
import string
import argparse
import os
from faker import Faker

# Thiết lập Faker để tạo dữ liệu ngẫu nhiên
fake = Faker('vi_VN')

def create_spark_session(jdbc_jar_path):
    """Khởi tạo SparkSession"""
    print("🚀 Đang khởi tạo SparkSession...")
    
    spark = SparkSession.builder \
        .appName("HDBank_PySpark_Benchmark") \
        .config("spark.jars", jdbc_jar_path) \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.default.parallelism", "8") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    print("✅ SparkSession đã được khởi tạo!")
    
    return spark

def generate_customer_data(spark, num_rows=10000):
    """Tạo dữ liệu khách hàng mẫu"""
    print(f"📊 Tạo {num_rows} khách hàng mẫu...")
    
    # Tạo danh sách khách hàng
    customer_data = []
    cities = ["TP.HCM", "Hà Nội", "Đà Nẵng", "Cần Thơ", "Hải Phòng", "Nha Trang", "Huế", "Vũng Tàu"]
    genders = ["Nam", "Nữ"]
    
    for i in range(1, num_rows + 1):
        customer_data.append((
            i,
            fake.name(),
            random.randint(18, 70),
            random.choice(genders),
            random.choice(cities),
            f"0{random.randint(900000000, 999999999)}",
            random.randint(1000000, 1000000000)
        ))
    
    # Tạo DataFrame
    customers_columns = ["id", "name", "age", "gender", "city", "phone", "balance"]
    customers_df = spark.createDataFrame(customer_data, customers_columns)
    
    print(f"✅ Đã tạo {num_rows} khách hàng mẫu!")
    return customers_df

def generate_transaction_data(spark, num_customers, transactions_per_customer=10):
    """Tạo dữ liệu giao dịch mẫu"""
    num_rows = num_customers * transactions_per_customer
    print(f"📊 Tạo {num_rows} giao dịch mẫu...")
    
    # Tạo danh sách giao dịch
    transaction_data = []
    transaction_types = ["deposit", "withdrawal", "transfer", "payment"]
    descriptions = [
        "Tiền lương", "Rút tiền mặt", "Chuyển tiền", "Tiết kiệm", 
        "Thanh toán hóa đơn", "Đầu tư", "Nhận tiền", "Trả nợ",
        "Tiền thưởng", "Tiền lãi"
    ]
    
    transaction_id = 1
    for i in range(1, num_customers + 1):
        for j in range(transactions_per_customer):
            transaction_type = random.choice(transaction_types)
            
            # Tạo số tiền phù hợp với loại giao dịch
            if transaction_type == "deposit":
                amount = random.randint(1000000, 50000000)
            elif transaction_type == "withdrawal":
                amount = random.randint(500000, 10000000)
            elif transaction_type == "transfer":
                amount = random.randint(100000, 20000000)
            else:  # payment
                amount = random.randint(50000, 5000000)
            
            # Tạo ngày giao dịch
            year = 2023
            month = random.randint(1, 12)
            day = random.randint(1, 28)
            date = f"{year}-{month:02d}-{day:02d}"
            
            transaction_data.append((
                transaction_id,
                i,  # customer_id
                date,
                transaction_type,
                amount,
                random.choice(descriptions)
            ))
            
            transaction_id += 1
    
    # Tạo DataFrame
    transactions_columns = ["id", "customer_id", "date", "type", "amount", "description"]
    transactions_df = spark.createDataFrame(transaction_data, transactions_columns)
    
    print(f"✅ Đã tạo {num_rows} giao dịch mẫu!")
    return transactions_df

def benchmark_sql_vs_dataframe(spark, customers_df, transactions_df, iterations=3):
    """So sánh hiệu suất giữa SQL và DataFrame API"""
    print("\n📊 Bắt đầu benchmark SQL vs DataFrame API...")
    
    # Đăng ký DataFrames như các bảng tạm để truy vấn SQL
    customers_df.createOrReplaceTempView("customers")
    transactions_df.createOrReplaceTempView("transactions")
    
    # Khởi tạo kết quả benchmark
    sql_times = []
    df_times = []
    
    for i in range(iterations):
        print(f"\n🔄 Lần chạy #{i+1}")
        
        # ------ PHẦN 1: TRUY VẤN SQL ------
        print("\n📊 [SQL] Thực hiện phân tích giao dịch...")
        sql_start_time = time.time()
        
        sql_query = """
            SELECT 
                c.id, 
                c.name, 
                c.city, 
                c.gender,
                COUNT(t.id) as transaction_count,
                SUM(CASE WHEN t.type = 'deposit' THEN t.amount ELSE 0 END) as total_deposits,
                SUM(CASE WHEN t.type = 'withdrawal' THEN t.amount ELSE 0 END) as total_withdrawals,
                SUM(CASE WHEN t.type = 'transfer' THEN t.amount ELSE 0 END) as total_transfers,
                SUM(CASE WHEN t.type = 'payment' THEN t.amount ELSE 0 END) as total_payments,
                SUM(CASE WHEN t.type = 'deposit' THEN t.amount 
                     WHEN t.type IN ('withdrawal', 'payment', 'transfer') THEN -t.amount 
                     ELSE 0 END) as net_flow,
                MIN(t.date) as first_transaction_date,
                MAX(t.date) as last_transaction_date
            FROM customers c
            LEFT JOIN transactions t ON c.id = t.customer_id
            GROUP BY c.id, c.name, c.city, c.gender
            ORDER BY net_flow DESC
        """
        
        sql_result = spark.sql(sql_query)
        # Thực hiện action để đảm bảo truy vấn được thực thi
        sql_result_count = sql_result.count()
        
        sql_execution_time = time.time() - sql_start_time
        sql_times.append(sql_execution_time)
        print(f"⏱️ Thời gian thực thi SQL: {sql_execution_time:.4f} giây")
        print(f"📊 Kết quả: {sql_result_count} dòng")
        
        # Hiển thị 5 kết quả đầu tiên
        print("Mẫu kết quả:")
        sql_result.show(5, truncate=False)
        
        # ------ PHẦN 2: DATAFRAME API ------
        print("\n📊 [DataFrame API] Thực hiện phân tích giao dịch...")
        df_start_time = time.time()
        
        from pyspark.sql.functions import min, max
        
        df_result = customers_df.join(
            transactions_df,
            customers_df.id == transactions_df.customer_id,
            "left"
        ).groupBy(
            customers_df.id,
            customers_df.name,
            customers_df.city,
            customers_df.gender
        ).agg(
            count(transactions_df.id).alias("transaction_count"),
            sum(when(transactions_df.type == "deposit", transactions_df.amount).otherwise(0)).alias("total_deposits"),
            sum(when(transactions_df.type == "withdrawal", transactions_df.amount).otherwise(0)).alias("total_withdrawals"),
            sum(when(transactions_df.type == "transfer", transactions_df.amount).otherwise(0)).alias("total_transfers"),
            sum(when(transactions_df.type == "payment", transactions_df.amount).otherwise(0)).alias("total_payments"),
            sum(when(transactions_df.type == "deposit", transactions_df.amount)
                .when(transactions_df.type.isin("withdrawal", "payment", "transfer"), -transactions_df.amount)
                .otherwise(0)).alias("net_flow"),
            min(transactions_df.date).alias("first_transaction_date"),
            max(transactions_df.date).alias("last_transaction_date")
        ).orderBy(desc("net_flow"))
        
        # Thực hiện action để đảm bảo truy vấn được thực thi
        df_result_count = df_result.count()
        
        df_execution_time = time.time() - df_start_time
        df_times.append(df_execution_time)
        print(f"⏱️ Thời gian thực thi DataFrame API: {df_execution_time:.4f} giây")
        print(f"📊 Kết quả: {df_result_count} dòng")
        
        # Hiển thị 5 kết quả đầu tiên
        print("Mẫu kết quả:")
        df_result.show(5, truncate=False)
    
    # Tính trung bình các lần chạy
    avg_sql_time = sum(sql_times) / len(sql_times)
    avg_df_time = sum(df_times) / len(df_times)
    
    # Hiển thị kết quả tổng hợp
    print("\n📈 KẾT QUẢ BENCHMARK TỔNG HỢP:")
    print(f"Số lượng khách hàng: {customers_df.count()}")
    print(f"Số lượng giao dịch: {transactions_df.count()}")
    print(f"Số lần chạy: {iterations}")
    print("\nThời gian thực thi trung bình:")
    print(f"   SQL:          {avg_sql_time:.4f} giây")
    print(f"   DataFrame:    {avg_df_time:.4f} giây")
    print(f"   Chênh lệch:   {abs(avg_sql_time - avg_df_time):.4f} giây")
    
    if avg_sql_time < avg_df_time:
        print(f"   👉 SQL nhanh hơn {(avg_df_time/avg_sql_time - 1)*100:.2f}%")
    else:
        print(f"   👉 DataFrame API nhanh hơn {(avg_sql_time/avg_df_time - 1)*100:.2f}%")
    
    return {
        "sql_times": sql_times,
        "df_times": df_times,
        "avg_sql_time": avg_sql_time,
        "avg_df_time": avg_df_time
    }

def write_to_postgres(customers_df, transactions_df, jdbc_url, connection_properties):
    """Ghi dữ liệu vào PostgreSQL"""
    print("\n📥 Đang ghi dữ liệu vào PostgreSQL...")
    
    # Ghi dữ liệu khách hàng
    print("📊 Ghi bảng customers...")
    start_time = time.time()
    customers_df.write \
        .mode("overwrite") \
        .jdbc(jdbc_url, "benchmark_customers", properties=connection_properties)
    print(f"✅ Hoàn thành trong {time.time() - start_time:.2f} giây")
    
    # Ghi dữ liệu giao dịch
    print("📊 Ghi bảng transactions...")
    start_time = time.time()
    transactions_df.write \
        .mode("overwrite") \
        .jdbc(jdbc_url, "benchmark_transactions", properties=connection_properties)
    print(f"✅ Hoàn thành trong {time.time() - start_time:.2f} giây")
    
    print("✅ Đã ghi dữ liệu vào PostgreSQL thành công!")

def benchmark_postgres_vs_spark(spark, jdbc_url, connection_properties, iterations=3):
    """So sánh hiệu suất truy vấn giữa PostgreSQL và PySpark"""
    print("\n📊 Bắt đầu benchmark PostgreSQL vs PySpark...")
    
    # Truy vấn trực tiếp từ PostgreSQL
    postgres_times = []
    spark_times = []
    
    for i in range(iterations):
        print(f"\n🔄 Lần chạy #{i+1}")
        
        # ------ PHẦN 1: TRUY VẤN POSTGRESQL ------
        print("\n📊 [PostgreSQL] Thực hiện truy vấn trực tiếp từ PostgreSQL...")
        pg_start_time = time.time()
        
        # Sử dụng pushdown query để thực thi truy vấn hoàn toàn trong PostgreSQL
        pushdown_query = """
        (SELECT 
            c.id, 
            c.name, 
            c.city, 
            c.gender,
            COUNT(t.id) as transaction_count,
            SUM(CASE WHEN t.type = 'deposit' THEN t.amount ELSE 0 END) as total_deposits,
            SUM(CASE WHEN t.type = 'withdrawal' THEN t.amount ELSE 0 END) as total_withdrawals,
            SUM(CASE WHEN t.type = 'transfer' THEN t.amount ELSE 0 END) as total_transfers,
            SUM(CASE WHEN t.type = 'payment' THEN t.amount ELSE 0 END) as total_payments,
            SUM(CASE WHEN t.type = 'deposit' THEN t.amount 
                 WHEN t.type IN ('withdrawal', 'payment', 'transfer') THEN -t.amount 
                 ELSE 0 END) as net_flow,
            MIN(t.date) as first_transaction_date,
            MAX(t.date) as last_transaction_date
        FROM benchmark_customers c
        LEFT JOIN benchmark_transactions t ON c.id = t.customer_id
        GROUP BY c.id, c.name, c.city, c.gender
        ORDER BY net_flow DESC) AS query_result
        """
        
        pg_result = spark.read \
            .jdbc(jdbc_url, pushdown_query, properties=connection_properties)
        
        # Thực hiện action để đảm bảo truy vấn được thực thi
        pg_result_count = pg_result.count()
        
        pg_execution_time = time.time() - pg_start_time
        postgres_times.append(pg_execution_time)
        print(f"⏱️ Thời gian truy vấn PostgreSQL: {pg_execution_time:.4f} giây")
        print(f"📊 Kết quả: {pg_result_count} dòng")
        
        # Hiển thị 5 kết quả đầu tiên
        print("Mẫu kết quả:")
        pg_result.show(5, truncate=False)
        
        # ------ PHẦN 2: TRUY VẤN SPARK ------
        print("\n📊 [Spark] Đọc dữ liệu từ PostgreSQL và xử lý trong PySpark...")
        spark_start_time = time.time()
        
        # Đọc dữ liệu từ PostgreSQL vào Spark
        customers_df = spark.read \
            .jdbc(jdbc_url, "benchmark_customers", properties=connection_properties)
        
        transactions_df = spark.read \
            .jdbc(jdbc_url, "benchmark_transactions", properties=connection_properties)
        
        # Đăng ký DataFrames như các bảng tạm để truy vấn SQL
        customers_df.createOrReplaceTempView("customers")
        transactions_df.createOrReplaceTempView("transactions")
        
        # Thực hiện truy vấn trong Spark
        spark_query = """
            SELECT 
                c.id, 
                c.name, 
                c.city, 
                c.gender,
                COUNT(t.id) as transaction_count,
                SUM(CASE WHEN t.type = 'deposit' THEN t.amount ELSE 0 END) as total_deposits,
                SUM(CASE WHEN t.type = 'withdrawal' THEN t.amount ELSE 0 END) as total_withdrawals,
                SUM(CASE WHEN t.type = 'transfer' THEN t.amount ELSE 0 END) as total_transfers,
                SUM(CASE WHEN t.type = 'payment' THEN t.amount ELSE 0 END) as total_payments,
                SUM(CASE WHEN t.type = 'deposit' THEN t.amount 
                     WHEN t.type IN ('withdrawal', 'payment', 'transfer') THEN -t.amount 
                     ELSE 0 END) as net_flow,
                MIN(t.date) as first_transaction_date,
                MAX(t.date) as last_transaction_date
            FROM customers c
            LEFT JOIN transactions t ON c.id = t.customer_id
            GROUP BY c.id, c.name, c.city, c.gender
            ORDER BY net_flow DESC
        """
        
        spark_result = spark.sql(spark_query)
        
        # Thực hiện action để đảm bảo truy vấn được thực thi
        spark_result_count = spark_result.count()
        
        spark_execution_time = time.time() - spark_start_time
        spark_times.append(spark_execution_time)
        print(f"⏱️ Thời gian xử lý PySpark: {spark_execution_time:.4f} giây")
        print(f"📊 Kết quả: {spark_result_count} dòng")
        
        # Hiển thị 5 kết quả đầu tiên
        print("Mẫu kết quả:")
        spark_result.show(5, truncate=False)
    
    # Tính trung bình các lần chạy
    avg_postgres_time = sum(postgres_times) / len(postgres_times)
    avg_spark_time = sum(spark_times) / len(spark_times)
    
    # Hiển thị kết quả tổng hợp
    print("\n📈 KẾT QUẢ BENCHMARK POSTGRESQL VS PYSPARK:")
    print(f"Số lần chạy: {iterations}")
    print("\nThời gian thực thi trung bình:")
    print(f"   PostgreSQL:   {avg_postgres_time:.4f} giây")
    print(f"   PySpark:      {avg_spark_time:.4f} giây")
    print(f"   Chênh lệch:   {abs(avg_postgres_time - avg_spark_time):.4f} giây")
    
    if avg_postgres_time < avg_spark_time:
        print(f"   👉 PostgreSQL nhanh hơn {(avg_spark_time/avg_postgres_time - 1)*100:.2f}%")
    else:
        print(f"   👉 PySpark nhanh hơn {(avg_postgres_time/avg_spark_time - 1)*100:.2f}%")
    
    return {
        "postgres_times": postgres_times,
        "spark_times": spark_times,
        "avg_postgres_time": avg_postgres_time,
        "avg_spark_time": avg_spark_time
    }

def main():
    parser = argparse.ArgumentParser(description='HDBank PySpark SQL vs DataFrame Benchmark')
    parser.add_argument('--jar', type=str, 
                        default="/home/devduongthanhdat/Desktop/hdbank/hdbank_pyspark/postgresql-42.7.2.jar",
                        help='Đường dẫn tới PostgreSQL JDBC JAR')
    parser.add_argument('--db', type=str, default="hdbank_db", 
                        help='Tên database PostgreSQL')
    parser.add_argument('--user', type=str, default="postgres", 
                        help='Username PostgreSQL')
    parser.add_argument('--password', type=str, default="postgres", 
                        help='Mật khẩu PostgreSQL')
    parser.add_argument('--customers', type=int, default=1000, 
                        help='Số lượng khách hàng để tạo')
    parser.add_argument('--transactions', type=int, default=10, 
                        help='Số lượng giao dịch trên mỗi khách hàng')
    parser.add_argument('--iterations', type=int, default=3, 
                        help='Số lần chạy mỗi benchmark')
    parser.add_argument('--write-to-postgres', action='store_true',
                        help='Ghi dữ liệu vào PostgreSQL để benchmark')
    parser.add_argument('--postgres-vs-spark', action='store_true',
                        help='So sánh hiệu suất giữa PostgreSQL và PySpark')
    
    args = parser.parse_args()
    
    # Thiết lập thông tin kết nối JDBC
    jdbc_url = f"jdbc:postgresql://localhost:5432/{args.db}"
    connection_properties = {
        "user": args.user,
        "password": args.password,
        "driver": "org.postgresql.Driver"
    }
    
    try:
        print("\n🏦 HDBANK: BENCHMARK PYSPARK SQL VS DATAFRAME API")
        print("=" * 60)
        
        # Kiểm tra file JAR tồn tại
        if not os.path.exists(args.jar):
            raise FileNotFoundError(f"Không tìm thấy JDBC JAR tại: {args.jar}")
        
        # Tạo Spark session
        spark = create_spark_session(args.jar)
        
        # Tạo dữ liệu mẫu
        customers_df = generate_customer_data(spark, args.customers)
        transactions_df = generate_transaction_data(spark, args.customers, args.transactions)
        
        # Benchmark SQL vs DataFrame API
        benchmark_results = benchmark_sql_vs_dataframe(spark, customers_df, transactions_df, args.iterations)
        
        # Nếu cần ghi vào PostgreSQL và benchmark
        if args.write_to_postgres:
            write_to_postgres(customers_df, transactions_df, jdbc_url, connection_properties)
            
            if args.postgres_vs_spark:
                pg_spark_results = benchmark_postgres_vs_spark(spark, jdbc_url, connection_properties, args.iterations)
        
        print("\n🎉 Benchmark hoàn tất!")
        
    except Exception as e:
        print(f"❌ Lỗi: {str(e)}")
        import traceback
        traceback.print_exc()
        
    finally:
        if 'spark' in locals():
            spark.stop()
            print("✅ Đã đóng SparkSession")

if __name__ == "__main__":
    main()
