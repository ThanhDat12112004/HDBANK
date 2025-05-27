#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HDBank PySpark PostgreSQL JDBC Demo
===================================
Chương trình minh họa tích hợp PySpark với PostgreSQL sử dụng JDBC
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, lit
import argparse
import time
import os

class PySparkPostgreSQLDemo:
    def __init__(self, jdbc_jar_path, db_name="hdbank_db", user="postgres", password="hdbank123"):
        """Khởi tạo PySpark Session và thiết lập kết nối PostgreSQL"""
        self.jdbc_jar_path = jdbc_jar_path
        self.db_name = db_name
        self.user = user
        self.password = password
        
        # Kiểm tra file JAR tồn tại
        if not os.path.exists(jdbc_jar_path):
            raise FileNotFoundError(f"Không tìm thấy JDBC JAR tại: {jdbc_jar_path}")
        
        # Khởi tạo SparkSession với JDBC driver
        print("🚀 Đang khởi tạo SparkSession...")
        self.spark = SparkSession.builder \
            .appName("HDBank_PostgreSQL_JDBC_Demo") \
            .config("spark.jars", jdbc_jar_path) \
            .getOrCreate()
        
        # Giảm log level để output dễ đọc hơn
        self.spark.sparkContext.setLogLevel("ERROR")
        
        # Thiết lập thông tin kết nối JDBC
        self.jdbc_url = f"jdbc:postgresql://localhost:5432/{db_name}"
        self.connection_properties = {
            "user": user,
            "password": password,
            "driver": "org.postgresql.Driver"
        }
        
        print("✅ SparkSession đã được khởi tạo!")
        print(f"📊 Kết nối đến PostgreSQL: {self.jdbc_url}")
    
    def create_demo_data(self):
        """Tạo dữ liệu mẫu: customers và transactions"""
        print("\n📊 Tạo dữ liệu mẫu...")
        
        # Tạo bảng khách hàng
        customers_data = [
            (1, "Nguyễn Văn An", 25, "Nam", "TP.HCM", "0901234567", 15000000.0),
            (2, "Trần Thị Bình", 30, "Nữ", "Hà Nội", "0918765432", 25000000.0),
            (3, "Lê Văn Cường", 28, "Nam", "Đà Nẵng", "0897654321", 8000000.0),
            (4, "Phạm Thị Dung", 35, "Nữ", "Cần Thơ", "0976543210", 42000000.0),
            (5, "Hoàng Văn Em", 40, "Nam", "TP.HCM", "0865432109", 67000000.0)
        ]
        customers_columns = ["id", "name", "age", "gender", "city", "phone", "balance"]
        customers_df = self.spark.createDataFrame(customers_data, customers_columns)
        
        # Tạo bảng giao dịch
        transactions_data = [
            (101, 1, "2023-03-01", "deposit", 5000000.0, "Tiền lương"),
            (102, 2, "2023-03-02", "withdrawal", 3000000.0, "Rút tiền mặt"),
            (103, 1, "2023-03-03", "transfer", 2000000.0, "Chuyển tiền"),
            (104, 3, "2023-03-05", "deposit", 10000000.0, "Tiết kiệm"),
            (105, 2, "2023-03-07", "payment", 1500000.0, "Thanh toán hóa đơn"),
            (106, 4, "2023-03-10", "deposit", 8000000.0, "Tiền lương"),
            (107, 5, "2023-03-12", "withdrawal", 10000000.0, "Rút tiền mặt"),
            (108, 3, "2023-03-15", "payment", 500000.0, "Thanh toán hóa đơn"),
            (109, 1, "2023-03-18", "transfer", 3000000.0, "Chuyển tiền"),
            (110, 5, "2023-03-20", "deposit", 15000000.0, "Đầu tư")
        ]
        transactions_columns = ["id", "customer_id", "date", "type", "amount", "description"]
        transactions_df = self.spark.createDataFrame(transactions_data, transactions_columns)
        
        # Ghi vào PostgreSQL
        try:
            customers_df.write \
                .mode("overwrite") \
                .jdbc(self.jdbc_url, "customers", properties=self.connection_properties)
                
            transactions_df.write \
                .mode("overwrite") \
                .jdbc(self.jdbc_url, "transactions", properties=self.connection_properties)
                
            print("✅ Tạo dữ liệu mẫu thành công!")
            
        except Exception as e:
            print(f"❌ Lỗi khi tạo dữ liệu mẫu: {str(e)}")
    
    def read_data(self, table_name):
        """Đọc dữ liệu từ PostgreSQL"""
        try:
            print(f"\n📖 Đọc dữ liệu từ bảng '{table_name}'...")
            start_time = time.time()
            
            df = self.spark.read \
                .jdbc(self.jdbc_url, table_name, properties=self.connection_properties)
            
            print(f"✅ Đọc thành công trong {time.time() - start_time:.4f} giây!")
            print(f"📋 Schema của bảng:")
            df.printSchema()
            print(f"📊 Dữ liệu ({df.count()} dòng):")
            df.show(truncate=False)
            
            return df
            
        except Exception as e:
            print(f"❌ Lỗi khi đọc dữ liệu: {str(e)}")
            return None

    def execute_sql_query(self, query, result_table="query_results"):
        """Thực thi truy vấn SQL và lưu kết quả vào PostgreSQL"""
        try:
            print(f"\n🔍 Thực thi truy vấn SQL:")
            print(f"   {query}")
            
            # Đăng ký bảng tạm để truy vấn SQL
            customers_df = self.spark.read \
                .jdbc(self.jdbc_url, "customers", properties=self.connection_properties)
            transactions_df = self.spark.read \
                .jdbc(self.jdbc_url, "transactions", properties=self.connection_properties)
            
            customers_df.createOrReplaceTempView("customers")
            transactions_df.createOrReplaceTempView("transactions")
            
            # Thực thi truy vấn
            start_time = time.time()
            result_df = self.spark.sql(query)
            
            print(f"✅ Truy vấn hoàn thành trong {time.time() - start_time:.4f} giây!")
            print(f"📊 Kết quả ({result_df.count()} dòng):")
            result_df.show(truncate=False)
            
            # Lưu kết quả vào PostgreSQL
            result_df.write \
                .mode("overwrite") \
                .jdbc(self.jdbc_url, result_table, properties=self.connection_properties)
            print(f"✅ Đã lưu kết quả vào bảng '{result_table}'")
            
            return result_df
            
        except Exception as e:
            print(f"❌ Lỗi khi thực thi truy vấn: {str(e)}")
            return None
    
    def update_data(self, table_name, condition_col, condition_value, update_col, update_value):
        """Cập nhật dữ liệu trong PostgreSQL"""
        try:
            print(f"\n🔄 Cập nhật dữ liệu trong bảng '{table_name}'...")
            
            # Đọc dữ liệu hiện tại
            df = self.spark.read \
                .jdbc(self.jdbc_url, table_name, properties=self.connection_properties)
            
            # Hiển thị dữ liệu trước khi cập nhật
            print("📊 Dữ liệu trước khi cập nhật:")
            df.filter(col(condition_col) == condition_value).show()
            
            # Thực hiện cập nhật
            updated_df = df.withColumn(
                update_col,
                when(col(condition_col) == condition_value, update_value).otherwise(col(update_col))
            )
            
            # Ghi lại dữ liệu đã cập nhật
            updated_df.write \
                .mode("overwrite") \
                .jdbc(self.jdbc_url, table_name, properties=self.connection_properties)
            
            # Hiển thị dữ liệu sau khi cập nhật
            print("✅ Cập nhật thành công!")
            print("📊 Dữ liệu sau khi cập nhật:")
            updated_df.filter(col(condition_col) == condition_value).show()
            
        except Exception as e:
            print(f"❌ Lỗi khi cập nhật dữ liệu: {str(e)}")
    
    def delete_data(self, table_name, condition_col, condition_value):
        """Xóa dữ liệu từ PostgreSQL"""
        try:
            print(f"\n🗑️ Xóa dữ liệu từ bảng '{table_name}'...")
            
            # Đọc dữ liệu hiện tại
            df = self.spark.read \
                .jdbc(self.jdbc_url, table_name, properties=self.connection_properties)
            
            # Hiển thị dữ liệu trước khi xóa
            print(f"📊 Số dòng trước khi xóa: {df.count()}")
            print("📊 Dữ liệu sẽ bị xóa:")
            df.filter(col(condition_col) == condition_value).show()
            
            # Thực hiện xóa
            filtered_df = df.filter(col(condition_col) != condition_value)
            
            # Ghi lại dữ liệu đã lọc
            filtered_df.write \
                .mode("overwrite") \
                .jdbc(self.jdbc_url, table_name, properties=self.connection_properties)
            
            # Hiển thị thông tin sau khi xóa
            print("✅ Xóa thành công!")
            print(f"📊 Số dòng sau khi xóa: {filtered_df.count()}")
            
        except Exception as e:
            print(f"❌ Lỗi khi xóa dữ liệu: {str(e)}")
    
    def join_data_demonstration(self):
        """Demo thao tác join dữ liệu giữa các bảng"""
        try:
            print("\n🔄 Demo thao tác join dữ liệu...")
            
            # Đọc dữ liệu từ các bảng
            customers_df = self.spark.read \
                .jdbc(self.jdbc_url, "customers", properties=self.connection_properties)
            
            transactions_df = self.spark.read \
                .jdbc(self.jdbc_url, "transactions", properties=self.connection_properties)
            
            # Đăng ký DataFrames như các bảng tạm để truy vấn SQL
            customers_df.createOrReplaceTempView("customers")
            transactions_df.createOrReplaceTempView("transactions")
            
            # ------ PHẦN 1: TRUY VẤN SỬ DỤNG SQL ------
            print("\n📊 [SQL] Tính tổng giao dịch theo từng khách hàng:")
            sql_start_time = time.time()
            query = """
                SELECT 
                    c.id, 
                    c.name, 
                    c.city, 
                    COUNT(t.id) as transaction_count,
                    SUM(CASE WHEN t.type = 'deposit' THEN t.amount ELSE 0 END) as total_deposits,
                    SUM(CASE WHEN t.type IN ('withdrawal', 'payment', 'transfer') THEN t.amount ELSE 0 END) as total_debits,
                    SUM(CASE WHEN t.type = 'deposit' THEN t.amount ELSE -t.amount END) as net_flow
                FROM customers c
                LEFT JOIN transactions t ON c.id = t.customer_id
                GROUP BY c.id, c.name, c.city
                ORDER BY net_flow DESC
            """
            
            sql_result_df = self.spark.sql(query)
            sql_execution_time = time.time() - sql_start_time
            print(f"⏱️ Thời gian thực thi SQL: {sql_execution_time:.4f} giây")
            sql_result_df.show(truncate=False)
            
            # ------ PHẦN 2: TRUY VẤN SỬ DỤNG DATAFRAME API ------
            print("\n📊 [DataFrame API] Tính tổng giao dịch theo từng khách hàng:")
            df_start_time = time.time()
            
            # Thực hiện join bằng DataFrame API
            from pyspark.sql.functions import sum, count, when, desc
            
            df_result = customers_df.join(
                transactions_df,
                customers_df.id == transactions_df.customer_id,
                "left"
            ).groupBy(
                customers_df.id,
                customers_df.name,
                customers_df.city
            ).agg(
                count(transactions_df.id).alias("transaction_count"),
                sum(when(transactions_df.type == "deposit", transactions_df.amount).otherwise(0)).alias("total_deposits"),
                sum(when(transactions_df.type.isin("withdrawal", "payment", "transfer"), transactions_df.amount).otherwise(0)).alias("total_debits"),
                sum(when(transactions_df.type == "deposit", transactions_df.amount).otherwise(-transactions_df.amount)).alias("net_flow")
            ).orderBy(desc("net_flow"))
            
            df_execution_time = time.time() - df_start_time
            print(f"⏱️ Thời gian thực thi DataFrame API: {df_execution_time:.4f} giây")
            df_result.show(truncate=False)
            
            # So sánh kết quả giữa 2 phương pháp
            print("\n📈 So sánh hiệu suất:")
            print(f"   SQL:          {sql_execution_time:.4f} giây")
            print(f"   DataFrame:    {df_execution_time:.4f} giây")
            print(f"   Chênh lệch:   {abs(sql_execution_time - df_execution_time):.4f} giây")
            
            if sql_execution_time < df_execution_time:
                print(f"   👉 SQL nhanh hơn {(df_execution_time/sql_execution_time - 1)*100:.2f}%")
            else:
                print(f"   👉 DataFrame API nhanh hơn {(sql_execution_time/df_execution_time - 1)*100:.2f}%")
            
            # Lưu kết quả phân tích vào PostgreSQL
            df_result.write \
                .mode("overwrite") \
                .jdbc(self.jdbc_url, "customer_transaction_summary", properties=self.connection_properties)
            
            print("✅ Đã lưu kết quả phân tích vào bảng 'customer_transaction_summary'")
            
            # ------ PHẦN 3: SO SÁNH TRUY VẤN TOP 3 -------
            print("\n📊 [SQL] Top 3 giao dịch lớn nhất:")
            sql_top_start = time.time()
            top_transactions_query = """
                SELECT 
                    t.id as transaction_id, 
                    c.name as customer_name,
                    t.date, 
                    t.type, 
                    t.amount, 
                    t.description
                FROM transactions t
                JOIN customers c ON t.customer_id = c.id
                ORDER BY t.amount DESC
                LIMIT 3
            """
            
            sql_top_df = self.spark.sql(top_transactions_query)
            sql_top_time = time.time() - sql_top_start
            print(f"⏱️ Thời gian thực thi SQL: {sql_top_time:.4f} giây")
            sql_top_df.show(truncate=False)
            
            print("\n📊 [DataFrame API] Top 3 giao dịch lớn nhất:")
            df_top_start = time.time()
            df_top = transactions_df.join(
                customers_df,
                transactions_df.customer_id == customers_df.id
            ).select(
                transactions_df.id.alias("transaction_id"),
                customers_df.name.alias("customer_name"),
                transactions_df.date,
                transactions_df.type,
                transactions_df.amount,
                transactions_df.description
            ).orderBy(desc("amount")).limit(3)
            
            df_top_time = time.time() - df_top_start
            print(f"⏱️ Thời gian thực thi DataFrame API: {df_top_time:.4f} giây")
            df_top.show(truncate=False)
            
            # So sánh kết quả giữa 2 phương pháp
            print("\n📈 So sánh hiệu suất (top 3):")
            print(f"   SQL:          {sql_top_time:.4f} giây")
            print(f"   DataFrame:    {df_top_time:.4f} giây")
            print(f"   Chênh lệch:   {abs(sql_top_time - df_top_time):.4f} giây")
            
            if sql_top_time < df_top_time:
                print(f"   👉 SQL nhanh hơn {(df_top_time/sql_top_time - 1)*100:.2f}%")
            else:
                print(f"   👉 DataFrame API nhanh hơn {(sql_top_time/df_top_time - 1)*100:.2f}%")
            
        except Exception as e:
            print(f"❌ Lỗi khi thực hiện demo join: {str(e)}")
            import traceback
            traceback.print_exc()
    
    def partition_demonstration(self):
        """Demo kỹ thuật tối ưu với partitioning khi làm việc với dữ liệu lớn"""
        try:
            print("\n🚀 Demo phân vùng dữ liệu để tối ưu hiệu suất...")
            
            # Đọc dữ liệu giao dịch từ PostgreSQL
            transactions_df = self.spark.read \
                .jdbc(self.jdbc_url, "transactions", properties=self.connection_properties)
            
            # Phân chia dữ liệu theo loại giao dịch
            print("\n📊 Phân vùng dữ liệu theo loại giao dịch...")
            
            # Lọc và xử lý từng loại giao dịch song song
            deposit_df = transactions_df.filter(col("type") == "deposit")
            withdrawal_df = transactions_df.filter(col("type") == "withdrawal")
            transfer_df = transactions_df.filter(col("type") == "transfer")
            payment_df = transactions_df.filter(col("type") == "payment")
            
            # Đếm số lượng giao dịch theo từng loại
            print(f"📊 Số giao dịch nạp tiền: {deposit_df.count()}")
            print(f"📊 Số giao dịch rút tiền: {withdrawal_df.count()}")
            print(f"📊 Số giao dịch chuyển tiền: {transfer_df.count()}")
            print(f"📊 Số giao dịch thanh toán: {payment_df.count()}")
            
            # Tính tổng giá trị giao dịch theo từng loại
            deposit_sum = deposit_df.selectExpr("sum(amount) as total_deposit").collect()[0]["total_deposit"]
            withdrawal_sum = withdrawal_df.selectExpr("sum(amount) as total_withdrawal").collect()[0]["total_withdrawal"]
            transfer_sum = transfer_df.selectExpr("sum(amount) as total_transfer").collect()[0]["total_transfer"]
            payment_sum = payment_df.selectExpr("sum(amount) as total_payment").collect()[0]["total_payment"]
            
            print(f"💰 Tổng tiền nạp: {deposit_sum:,.2f} VND")
            print(f"💰 Tổng tiền rút: {withdrawal_sum:,.2f} VND")
            print(f"💰 Tổng tiền chuyển: {transfer_sum:,.2f} VND")
            print(f"💰 Tổng tiền thanh toán: {payment_sum:,.2f} VND")
            
            # Demo tối ưu write với partitioning
            print("\n📊 Ghi dữ liệu với partitioning theo loại giao dịch...")
            
            # Thêm dữ liệu ngẫu nhiên để demo partitioning
            transactions_df = transactions_df.withColumn("month", expr("substring(date, 6, 2)"))
            
            # Ghi với partitioning theo tháng vào PostgreSQL
            transactions_df.write \
                .partitionBy("month") \
                .mode("overwrite") \
                .jdbc(self.jdbc_url, "transactions_partitioned", properties=self.connection_properties)
            
            print("✅ Đã ghi dữ liệu với partitioning!")
            
            # Demo đọc dữ liệu với pushdown filter
            print("\n📊 Demo tối ưu đọc dữ liệu với pushdown filter...")
            
            # Thiết lập pushdown query để thực thi ở phía database server
            pushdown_query = "(SELECT * FROM transactions WHERE amount > 5000000) AS filtered_data"
            
            start_time = time.time()
            filtered_df = self.spark.read \
                .jdbc(
                    self.jdbc_url, 
                    pushdown_query, 
                    properties=self.connection_properties
                )
            
            print(f"✅ Đọc dữ liệu với pushdown filter hoàn thành trong {time.time() - start_time:.4f} giây!")
            print(f"📊 Kết quả ({filtered_df.count()} dòng):")
            filtered_df.show(truncate=False)
            
        except Exception as e:
            print(f"❌ Lỗi khi thực hiện demo partition: {str(e)}")
    
    def run_full_demo(self):
        """Chạy toàn bộ demo"""
        print("\n🎮 Bắt đầu chạy DEMO đầy đủ...")
        
        # Tạo dữ liệu mẫu
        self.create_demo_data()
        
        # Hiển thị dữ liệu
        self.read_data("customers")
        self.read_data("transactions")
        
        # Demo SQL Query
        self.execute_sql_query(
            """
            SELECT c.name, c.city, c.balance,
                   COUNT(t.id) as transaction_count,
                   SUM(t.amount) as total_transaction_amount
            FROM customers c
            JOIN transactions t ON c.id = t.customer_id
            GROUP BY c.name, c.city, c.balance
            ORDER BY total_transaction_amount DESC
            """,
            "customer_transaction_analysis"
        )
        
        # Demo cập nhật dữ liệu
        self.update_data("customers", "id", 1, "balance", 20000000.0)
        
        # Demo xóa dữ liệu
        # self.delete_data("transactions", "id", 110)
        
        # Demo join dữ liệu
        self.join_data_demonstration()
        
        # Demo partition
        self.partition_demonstration()
        
        print("\n🎉 Demo hoàn tất!")
    
    def close(self):
        """Đóng SparkSession"""
        if self.spark:
            self.spark.stop()
            print("✅ Đã đóng SparkSession")


def main():
    parser = argparse.ArgumentParser(description='HDBank PySpark PostgreSQL JDBC Demo')
    parser.add_argument('--jar', type=str, 
                        default="/home/devduongthanhdat/Desktop/hdbank/hdbank_pyspark/postgresql-42.7.2.jar",
                        help='Đường dẫn tới PostgreSQL JDBC JAR')
    parser.add_argument('--db', type=str, default="hdbank_db", 
                        help='Tên database PostgreSQL')
    parser.add_argument('--user', type=str, default="postgres", 
                        help='Username PostgreSQL')
    parser.add_argument('--password', type=str, default="hdbank123", 
                        help='Mật khẩu PostgreSQL')
    
    args = parser.parse_args()
    
    try:
        # Khởi tạo demo
        demo = PySparkPostgreSQLDemo(
            jdbc_jar_path=args.jar,
            db_name=args.db,
            user=args.user,
            password=args.password
        )
        
        # Chạy demo đầy đủ
        demo.run_full_demo()
        
    except Exception as e:
        print(f"❌ Lỗi: {str(e)}")
        
    finally:
        if 'demo' in locals():
            demo.close()

if __name__ == "__main__":
    main()