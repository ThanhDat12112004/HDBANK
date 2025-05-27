#!/usr/bin/env python3
"""
HDBank Data Platform Integration
This script provides a comprehensive demonstration of a modern data platform integrating:
1. HDFS for distributed storage
2. PostgreSQL for relational data
3. PySpark for data processing and analytics

It demonstrates data flow between systems and various analytics scenarios.
"""

import os
import time
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Environment setup
HADOOP_HOME = "/home/devduongthanhdat/Desktop/hdbank/hadoop-3.3.6"
JAVA_HOME = "/usr/lib/jvm/java-17-openjdk-amd64"
JDBC_JAR = "/home/devduongthanhdat/Desktop/hdbank/hdbank_pyspark/postgresql-42.7.2.jar"

# PostgreSQL configuration
DB_CONFIG = {
    "url": "jdbc:postgresql://localhost:5432/hdbank_db",
    "driver": "org.postgresql.Driver",
    "user": "postgres",
    "password": "postgres"
}

# HDFS paths
HDFS_BASE = "hdfs://localhost:9000"
HDFS_USER_DIR = f"{HDFS_BASE}/user/devduongthanhdat"
HDFS_DATA_LAKE = f"{HDFS_USER_DIR}/data_lake"
HDFS_ANALYTICS = f"{HDFS_USER_DIR}/analytics_results"

def setup_environment():
    """Set up environment variables"""
    os.environ["HADOOP_HOME"] = HADOOP_HOME
    os.environ["JAVA_HOME"] = JAVA_HOME
    os.environ["HADOOP_CONF_DIR"] = f"{HADOOP_HOME}/etc/hadoop"
    
    print("🔧 Environment setup complete:")
    print(f"  HADOOP_HOME: {HADOOP_HOME}")
    print(f"  JAVA_HOME: {JAVA_HOME}")
    print(f"  JDBC JAR: {JDBC_JAR}")

def create_spark_session():
    """Create a Spark session with HDFS and JDBC configuration"""
    print("🚀 Creating PySpark session...")
    
    spark = SparkSession.builder \
        .appName("HDBank Data Platform") \
        .config("spark.jars", JDBC_JAR) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.hadoop.fs.defaultFS", HDFS_BASE) \
        .config("spark.sql.warehouse.dir", f"{HDFS_BASE}/user/spark-warehouse") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")  # Reduce log verbosity
    
    print("✅ PySpark session created successfully")
    return spark

def create_test_data(spark):
    """Create test data for demonstrations"""
    print("📊 Generating test data...")
    
    # Generate customer data
    customer_data = [
        (101, "Nguyen Van A", "HCM", "0901234567", "nguyen.van.a@email.com", 25000000),
        (102, "Tran Thi B", "HN", "0902345678", "tran.thi.b@email.com", 15000000),
        (103, "Le Van C", "DN", "0903456789", "le.van.c@email.com", 35000000),
        (104, "Pham Thi D", "HCM", "0904567890", "pham.thi.d@email.com", 45000000),
        (105, "Hoang Van E", "HN", "0905678901", "hoang.van.e@email.com", 55000000),
        (106, "Vu Thi F", "CT", "0906789012", "vu.thi.f@email.com", 30000000),
        (107, "Dao Van G", "HCM", "0907890123", "dao.van.g@email.com", 20000000),
        (108, "Bui Thi H", "HN", "0908901234", "bui.thi.h@email.com", 40000000),
        (109, "Ngo Van I", "DN", "0909012345", "ngo.van.i@email.com", 60000000),
        (110, "Dang Thi K", "HCM", "0900123456", "dang.thi.k@email.com", 50000000)
    ]
    
    customer_schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("customer_name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("email", StringType(), True),
        StructField("balance", LongType(), True)
    ])
    
    customers_df = spark.createDataFrame(customer_data, customer_schema)
    
    # Generate transaction data
    transaction_data = []
    transaction_id = 1001
    
    # Tạo nhiều giao dịch cho mỗi khách hàng
    for customer_id in range(101, 111):
        # Giao dịch gửi tiền
        transaction_data.append(
            (transaction_id, customer_id, "2024-01-15", "DEPOSIT", 5000000, "ATM")
        )
        transaction_id += 1
        
        # Giao dịch rút tiền
        transaction_data.append(
            (transaction_id, customer_id, "2024-01-20", "WITHDRAWAL", -1000000, "BRANCH")
        )
        transaction_id += 1
        
        # Giao dịch chuyển tiền
        transaction_data.append(
            (transaction_id, customer_id, "2024-01-25", "TRANSFER", -2000000, "ONLINE")
        )
        transaction_id += 1
        
        # Giao dịch nhận tiền
        transaction_data.append(
            (transaction_id, customer_id, "2024-01-30", "RECEIVE", 3000000, "ONLINE")
        )
        transaction_id += 1
    
    transaction_schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("amount", LongType(), True),
        StructField("channel", StringType(), True)
    ])
    
    transactions_df = spark.createDataFrame(transaction_data, transaction_schema)
    
    # Generate product data
    product_data = [
        (201, "Savings Account", "SAVINGS", 0.04, 0, 50000),
        (202, "Premium Savings", "SAVINGS", 0.05, 100000000, 100000),
        (203, "Checking Account", "CHECKING", 0.01, 0, 0),
        (204, "Premium Checking", "CHECKING", 0.02, 50000000, 0),
        (205, "Personal Loan", "LOAN", 0.08, 10000000, 1000000),
        (206, "Home Loan", "LOAN", 0.07, 500000000, 5000000),
        (207, "Car Loan", "LOAN", 0.09, 100000000, 2000000),
        (208, "Fixed Deposit 3M", "TERM", 0.045, 10000000, 10000000),
        (209, "Fixed Deposit 6M", "TERM", 0.05, 10000000, 10000000),
        (210, "Fixed Deposit 12M", "TERM", 0.06, 10000000, 10000000)
    ]
    
    product_schema = StructType([
        StructField("product_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("product_type", StringType(), True),
        StructField("interest_rate", DoubleType(), True),
        StructField("min_amount", LongType(), True),
        StructField("min_balance", LongType(), True)
    ])
    
    products_df = spark.createDataFrame(product_data, product_schema)
    
    # Generate customer product mapping
    customer_product_data = [
        (101, 201, "2024-01-10", "ACTIVE", 25000000),
        (101, 203, "2024-01-10", "ACTIVE", 5000000),
        (102, 201, "2024-02-15", "ACTIVE", 15000000),
        (103, 202, "2024-01-20", "ACTIVE", 120000000),
        (103, 204, "2024-01-20", "ACTIVE", 15000000),
        (104, 201, "2024-03-05", "ACTIVE", 30000000),
        (104, 205, "2024-03-10", "ACTIVE", 50000000),
        (105, 202, "2024-02-18", "ACTIVE", 150000000),
        (106, 201, "2024-01-25", "ACTIVE", 30000000),
        (107, 203, "2024-01-15", "ACTIVE", 20000000),
        (108, 208, "2024-02-28", "ACTIVE", 100000000),
        (109, 210, "2024-03-15", "ACTIVE", 200000000),
        (110, 206, "2024-01-10", "ACTIVE", 1500000000)
    ]
    
    customer_product_schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("start_date", StringType(), True),
        StructField("status", StringType(), True),
        StructField("balance", LongType(), True)
    ])
    
    customer_products_df = spark.createDataFrame(customer_product_data, customer_product_schema)
    
    print("✅ Test data generated successfully")
    
    return {
        "customers": customers_df,
        "transactions": transactions_df,
        "products": products_df,
        "customer_products": customer_products_df
    }

def save_to_postgres(spark, dataframes):
    """Save dataframes to PostgreSQL"""
    print("\n🗄️ Saving data to PostgreSQL...")
    
    for name, df in dataframes.items():
        print(f"  📝 Saving {name} table...")
        
        df.write \
            .format("jdbc") \
            .option("url", DB_CONFIG["url"]) \
            .option("driver", DB_CONFIG["driver"]) \
            .option("dbtable", name) \
            .option("user", DB_CONFIG["user"]) \
            .option("password", DB_CONFIG["password"]) \
            .mode("overwrite") \
            .save()
    
    print("✅ Data saved to PostgreSQL successfully")

def save_to_hdfs(dataframes):
    """Save dataframes to HDFS"""
    print("\n💾 Saving data to HDFS data lake...")
    
    for name, df in dataframes.items():
        hdfs_path = f"{HDFS_DATA_LAKE}/{name}"
        print(f"  📝 Saving {name} to {hdfs_path}...")
        
        # Save as Parquet format (optimal for analytics)
        df.write.mode("overwrite").parquet(hdfs_path)
    
    print("✅ Data saved to HDFS successfully")

def run_basic_analytics(spark):
    """Run basic analytics on the data"""
    print("\n🔍 Running basic analytics...")
    
    # Load data from HDFS
    print("  📖 Loading data from HDFS...")
    customers = spark.read.parquet(f"{HDFS_DATA_LAKE}/customers")
    transactions = spark.read.parquet(f"{HDFS_DATA_LAKE}/transactions")
    products = spark.read.parquet(f"{HDFS_DATA_LAKE}/products")
    customer_products = spark.read.parquet(f"{HDFS_DATA_LAKE}/customer_products")
    
    # Register temp views for SQL queries
    customers.createOrReplaceTempView("customers")
    transactions.createOrReplaceTempView("transactions")
    products.createOrReplaceTempView("products")
    customer_products.createOrReplaceTempView("customer_products")
    
    # 1. Customer city distribution
    print("\n📊 Customer city distribution:")
    city_analysis = spark.sql("""
        SELECT 
            city, 
            COUNT(*) as customer_count,
            SUM(balance) as total_balance,
            AVG(balance) as avg_balance
        FROM customers
        GROUP BY city
        ORDER BY customer_count DESC
    """)
    
    city_analysis.show()
    
    # 2. Transaction analysis
    print("\n💳 Transaction analysis by type:")
    tx_analysis = spark.sql("""
        SELECT 
            transaction_type, 
            COUNT(*) as tx_count,
            SUM(amount) as total_amount,
            AVG(amount) as avg_amount
        FROM transactions
        GROUP BY transaction_type
        ORDER BY tx_count DESC
    """)
    
    tx_analysis.show()
    
    # 3. Product popularity
    print("\n🏆 Product popularity:")
    product_analysis = spark.sql("""
        SELECT 
            p.product_name,
            p.product_type,
            COUNT(cp.customer_id) as customer_count,
            SUM(cp.balance) as total_balance
        FROM products p
        JOIN customer_products cp ON p.product_id = cp.product_id
        GROUP BY p.product_name, p.product_type
        ORDER BY customer_count DESC
    """)
    
    product_analysis.show()
    
    # 4. City and product type analysis
    print("\n🌆 City and product type analysis:")
    city_product_analysis = spark.sql("""
        SELECT 
            c.city,
            p.product_type,
            COUNT(DISTINCT c.customer_id) as customer_count,
            SUM(cp.balance) as total_balance
        FROM customers c
        JOIN customer_products cp ON c.customer_id = cp.customer_id
        JOIN products p ON cp.product_id = p.product_id
        GROUP BY c.city, p.product_type
        ORDER BY c.city, total_balance DESC
    """)
    
    city_product_analysis.show()
    
    # Save analytics results to HDFS
    print("\n💾 Saving analytics results to HDFS...")
    
    city_analysis.write.mode("overwrite").parquet(f"{HDFS_ANALYTICS}/city_analysis")
    tx_analysis.write.mode("overwrite").parquet(f"{HDFS_ANALYTICS}/transaction_analysis")
    product_analysis.write.mode("overwrite").parquet(f"{HDFS_ANALYTICS}/product_analysis")
    city_product_analysis.write.mode("overwrite").parquet(f"{HDFS_ANALYTICS}/city_product_analysis")
    
    # Return analytics results
    return {
        "city_analysis": city_analysis,
        "transaction_analysis": tx_analysis,
        "product_analysis": product_analysis,
        "city_product_analysis": city_product_analysis
    }

def run_advanced_analytics(spark):
    """Run advanced analytics combining data from multiple sources"""
    print("\n🔬 Running advanced analytics...")
    
    # Load data
    print("  📖 Loading data from HDFS...")
    customers = spark.read.parquet(f"{HDFS_DATA_LAKE}/customers")
    transactions = spark.read.parquet(f"{HDFS_DATA_LAKE}/transactions")
    products = spark.read.parquet(f"{HDFS_DATA_LAKE}/products")
    customer_products = spark.read.parquet(f"{HDFS_DATA_LAKE}/customer_products")
    
    # Register temp views for SQL queries
    customers.createOrReplaceTempView("customers")
    transactions.createOrReplaceTempView("transactions")
    products.createOrReplaceTempView("products")
    customer_products.createOrReplaceTempView("customer_products")
    
    # 1. Customer value segmentation
    print("\n💰 Customer value segmentation:")
    customer_segments = spark.sql("""
        WITH customer_total_balance AS (
            SELECT
                c.customer_id,
                c.customer_name,
                c.city,
                c.balance as account_balance,
                SUM(cp.balance) as product_balance,
                (c.balance + SUM(cp.balance)) as total_balance
            FROM customers c
            JOIN customer_products cp ON c.customer_id = cp.customer_id
            GROUP BY c.customer_id, c.customer_name, c.city, c.balance
        ),
        customer_segment AS (
            SELECT
                *,
                CASE
                    WHEN total_balance >= 500000000 THEN 'PREMIUM'
                    WHEN total_balance >= 100000000 THEN 'GOLD'
                    WHEN total_balance >= 50000000 THEN 'SILVER'
                    ELSE 'REGULAR'
                END as segment
            FROM customer_total_balance
        )
        
        SELECT
            segment,
            COUNT(*) as customer_count,
            SUM(total_balance) as segment_balance,
            AVG(total_balance) as avg_balance
        FROM customer_segment
        GROUP BY segment
        ORDER BY segment_balance DESC
    """)
    
    customer_segments.show()
    
    # 2. Product portfolio analysis
    print("\n📈 Product portfolio analysis:")
    product_portfolio = spark.sql("""
        SELECT
            c.customer_id,
            c.customer_name,
            COUNT(DISTINCT p.product_type) as product_types,
            COUNT(p.product_id) as total_products,
            SUM(cp.balance) as portfolio_value
        FROM customers c
        JOIN customer_products cp ON c.customer_id = cp.customer_id
        JOIN products p ON cp.product_id = p.product_id
        GROUP BY c.customer_id, c.customer_name
        ORDER BY portfolio_value DESC
    """)
    
    product_portfolio.show()
    
    # 3. Transaction patterns
    print("\n📊 Transaction patterns by city:")
    tx_patterns = spark.sql("""
        SELECT
            c.city,
            t.transaction_type,
            t.channel,
            COUNT(*) as tx_count,
            SUM(t.amount) as total_amount,
            AVG(t.amount) as avg_amount
        FROM transactions t
        JOIN customers c ON t.customer_id = c.customer_id
        GROUP BY c.city, t.transaction_type, t.channel
        ORDER BY c.city, tx_count DESC
    """)
    
    tx_patterns.show()
    
    # 4. Customer product recommendations (based on city patterns)
    print("\n🎯 Customer product recommendations:")
    product_recommendations = spark.sql("""
        WITH city_product_popularity AS (
            SELECT
                c.city,
                p.product_type,
                p.product_id,
                p.product_name,
                COUNT(DISTINCT cp.customer_id) as customer_count,
                ROW_NUMBER() OVER (PARTITION BY c.city, p.product_type ORDER BY COUNT(DISTINCT cp.customer_id) DESC) as rank
            FROM customers c
            JOIN customer_products cp ON c.customer_id = cp.customer_id
            JOIN products p ON cp.product_id = p.product_id
            GROUP BY c.city, p.product_type, p.product_id, p.product_name
        ),
        target_customers AS (
            SELECT
                c.customer_id,
                c.customer_name,
                c.city,
                c.balance,
                COLLECT_SET(p.product_type) as owned_product_types
            FROM customers c
            LEFT JOIN customer_products cp ON c.customer_id = cp.customer_id
            LEFT JOIN products p ON cp.product_id = p.product_id
            GROUP BY c.customer_id, c.customer_name, c.city, c.balance
        )
        
        SELECT
            tc.customer_id,
            tc.customer_name,
            tc.city,
            tc.balance,
            cpp.product_type as recommended_product_type,
            cpp.product_name as recommended_product
        FROM target_customers tc
        JOIN city_product_popularity cpp ON tc.city = cpp.city AND cpp.rank = 1
        WHERE NOT array_contains(tc.owned_product_types, cpp.product_type)
        ORDER BY tc.customer_id
    """)
    
    product_recommendations.show()
    
    # Save advanced analytics to HDFS
    print("\n💾 Saving advanced analytics to HDFS...")
    
    customer_segments.write.mode("overwrite").parquet(f"{HDFS_ANALYTICS}/customer_segments")
    product_portfolio.write.mode("overwrite").parquet(f"{HDFS_ANALYTICS}/product_portfolio")
    tx_patterns.write.mode("overwrite").parquet(f"{HDFS_ANALYTICS}/tx_patterns")
    product_recommendations.write.mode("overwrite").parquet(f"{HDFS_ANALYTICS}/product_recommendations")
    
    # Save results to PostgreSQL for reporting
    print("\n🗄️ Saving analytics results to PostgreSQL...")
    
    customer_segments.write \
        .format("jdbc") \
        .option("url", DB_CONFIG["url"]) \
        .option("driver", DB_CONFIG["driver"]) \
        .option("dbtable", "customer_segments") \
        .option("user", DB_CONFIG["user"]) \
        .option("password", DB_CONFIG["password"]) \
        .mode("overwrite") \
        .save()
    
    product_recommendations.write \
        .format("jdbc") \
        .option("url", DB_CONFIG["url"]) \
        .option("driver", DB_CONFIG["driver"]) \
        .option("dbtable", "product_recommendations") \
        .option("user", DB_CONFIG["user"]) \
        .option("password", DB_CONFIG["password"]) \
        .mode("overwrite") \
        .save()
    
    return {
        "customer_segments": customer_segments,
        "product_portfolio": product_portfolio,
        "tx_patterns": tx_patterns,
        "product_recommendations": product_recommendations
    }

def compare_hdfs_postgres_performance(spark, dataframes):
    """Compare performance between HDFS and PostgreSQL"""
    print("\n⚡ HDFS vs PostgreSQL Performance Comparison...")
    
    customers_df = dataframes["customers"]
    transactions_df = dataframes["transactions"]
    
    # Create larger dataset for benchmark
    print("  📊 Creating larger dataset for benchmark...")
    
    # Expand customers dataset
    large_customers = customers_df
    for i in range(5):  # Multiply by 32
        large_customers = large_customers.union(large_customers)
    
    # Expand transactions dataset
    large_transactions = transactions_df
    for i in range(5):  # Multiply by 32
        large_transactions = large_transactions.union(large_transactions)
    
    print(f"  📈 Benchmark dataset size:")
    print(f"    Customers: {large_customers.count()} rows")
    print(f"    Transactions: {large_transactions.count()} rows")
    
    # Test HDFS write
    print("\n  💾 Writing to HDFS...")
    hdfs_write_start = time.time()
    
    large_customers.write.mode("overwrite").parquet(f"{HDFS_USER_DIR}/benchmark/customers")
    large_transactions.write.mode("overwrite").parquet(f"{HDFS_USER_DIR}/benchmark/transactions")
    
    hdfs_write_time = time.time() - hdfs_write_start
    print(f"  ✅ HDFS write completed in {hdfs_write_time:.2f} seconds")
    
    # Test PostgreSQL write
    print("\n  🗄️ Writing to PostgreSQL...")
    pg_write_start = time.time()
    
    large_customers.write \
        .format("jdbc") \
        .option("url", DB_CONFIG["url"]) \
        .option("driver", DB_CONFIG["driver"]) \
        .option("dbtable", "benchmark_customers") \
        .option("user", DB_CONFIG["user"]) \
        .option("password", DB_CONFIG["password"]) \
        .mode("overwrite") \
        .save()
    
    large_transactions.write \
        .format("jdbc") \
        .option("url", DB_CONFIG["url"]) \
        .option("driver", DB_CONFIG["driver"]) \
        .option("dbtable", "benchmark_transactions") \
        .option("user", DB_CONFIG["user"]) \
        .option("password", DB_CONFIG["password"]) \
        .mode("overwrite") \
        .save()
    
    pg_write_time = time.time() - pg_write_start
    print(f"  ✅ PostgreSQL write completed in {pg_write_time:.2f} seconds")
    
    # Test HDFS read
    print("\n  📖 Reading from HDFS...")
    hdfs_read_start = time.time()
    
    hdfs_customers = spark.read.parquet(f"{HDFS_USER_DIR}/benchmark/customers")
    hdfs_transactions = spark.read.parquet(f"{HDFS_USER_DIR}/benchmark/transactions")
    
    # Force action to read data
    hdfs_customers_count = hdfs_customers.count()
    hdfs_transactions_count = hdfs_transactions.count()
    
    hdfs_read_time = time.time() - hdfs_read_start
    print(f"  ✅ HDFS read completed in {hdfs_read_time:.2f} seconds")
    
    # Test PostgreSQL read
    print("\n  📖 Reading from PostgreSQL...")
    pg_read_start = time.time()
    
    pg_customers = spark.read \
        .format("jdbc") \
        .option("url", DB_CONFIG["url"]) \
        .option("driver", DB_CONFIG["driver"]) \
        .option("dbtable", "benchmark_customers") \
        .option("user", DB_CONFIG["user"]) \
        .option("password", DB_CONFIG["password"]) \
        .load()
    
    pg_transactions = spark.read \
        .format("jdbc") \
        .option("url", DB_CONFIG["url"]) \
        .option("driver", DB_CONFIG["driver"]) \
        .option("dbtable", "benchmark_transactions") \
        .option("user", DB_CONFIG["user"]) \
        .option("password", DB_CONFIG["password"]) \
        .load()
    
    # Force action to read data
    pg_customers_count = pg_customers.count()
    pg_transactions_count = pg_transactions.count()
    
    pg_read_time = time.time() - pg_read_start
    print(f"  ✅ PostgreSQL read completed in {pg_read_time:.2f} seconds")
    
    # Test HDFS query performance
    print("\n  🔍 Running analytics query on HDFS data...")
    hdfs_query_start = time.time()
    
    hdfs_result = hdfs_customers.join(
        hdfs_transactions,
        hdfs_customers.customer_id == hdfs_transactions.customer_id,
        "inner"
    ).groupBy(
        hdfs_customers.city,
        hdfs_transactions.transaction_type
    ).agg(
        count("*").alias("transaction_count"),
        sum(hdfs_transactions.amount).alias("total_amount")
    ).orderBy("city", desc("transaction_count"))
    
    hdfs_result_count = hdfs_result.count()  # Force execution
    
    hdfs_query_time = time.time() - hdfs_query_start
    print(f"  ✅ HDFS query completed in {hdfs_query_time:.2f} seconds")
    
    # Test PostgreSQL query performance
    print("\n  🔍 Running analytics query on PostgreSQL data...")
    pg_query_start = time.time()
    
    pg_result = pg_customers.join(
        pg_transactions,
        pg_customers.customer_id == pg_transactions.customer_id,
        "inner"
    ).groupBy(
        pg_customers.city,
        pg_transactions.transaction_type
    ).agg(
        count("*").alias("transaction_count"),
        sum(pg_transactions.amount).alias("total_amount")
    ).orderBy("city", desc("transaction_count"))
    
    pg_result_count = pg_result.count()  # Force execution
    
    pg_query_time = time.time() - pg_query_start
    print(f"  ✅ PostgreSQL query completed in {pg_query_time:.2f} seconds")
    
    # Compare performance
    print("\n📊 Performance Summary:")
    print(f"{'Operation':<15} {'HDFS (s)':<12} {'PostgreSQL (s)':<15} {'Ratio':<10}")
    print("-" * 55)
    print(f"{'Write':<15} {hdfs_write_time:<12.2f} {pg_write_time:<15.2f} {pg_write_time/hdfs_write_time if hdfs_write_time > 0 else 'N/A':<10.2f}x")
    print(f"{'Read':<15} {hdfs_read_time:<12.2f} {pg_read_time:<15.2f} {pg_read_time/hdfs_read_time if hdfs_read_time > 0 else 'N/A':<10.2f}x")
    print(f"{'Query':<15} {hdfs_query_time:<12.2f} {pg_query_time:<15.2f} {pg_query_time/hdfs_query_time if hdfs_query_time > 0 else 'N/A':<10.2f}x")
    
    # Write results to HDFS
    performance_results = spark.createDataFrame([
        ("Write", float(hdfs_write_time), float(pg_write_time), float(pg_write_time/hdfs_write_time) if hdfs_write_time > 0 else None),
        ("Read", float(hdfs_read_time), float(pg_read_time), float(pg_read_time/hdfs_read_time) if hdfs_read_time > 0 else None),
        ("Query", float(hdfs_query_time), float(pg_query_time), float(pg_query_time/hdfs_query_time) if hdfs_query_time > 0 else None)
    ], ["operation", "hdfs_time", "postgres_time", "ratio"])
    
    performance_results.write.mode("overwrite").parquet(f"{HDFS_ANALYTICS}/performance_comparison")
    
    return performance_results

def check_hdfs_status():
    """Check if HDFS is running and create directories if needed"""
    print("\n🔍 Checking HDFS status...")
    
    # Check if HDFS directories exist, create if they don't
    import subprocess
    
    # Function to run HDFS commands
    def run_hdfs_cmd(cmd):
        full_cmd = f"{HADOOP_HOME}/bin/hdfs dfs {cmd}"
        result = subprocess.run(full_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return result.returncode == 0
    
    # Check and create user directory
    if not run_hdfs_cmd(f"-test -d {HDFS_USER_DIR}"):
        print(f"  📁 Creating user directory: {HDFS_USER_DIR}")
        run_hdfs_cmd(f"-mkdir -p {HDFS_USER_DIR}")
    
    # Check and create data lake directory
    if not run_hdfs_cmd(f"-test -d {HDFS_DATA_LAKE}"):
        print(f"  📁 Creating data lake directory: {HDFS_DATA_LAKE}")
        run_hdfs_cmd(f"-mkdir -p {HDFS_DATA_LAKE}")
    
    # Check and create analytics directory
    if not run_hdfs_cmd(f"-test -d {HDFS_ANALYTICS}"):
        print(f"  📁 Creating analytics directory: {HDFS_ANALYTICS}")
        run_hdfs_cmd(f"-mkdir -p {HDFS_ANALYTICS}")
    
    print("✅ HDFS directories ready")

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description="HDBank Data Platform Demo")
    parser.add_argument("--prepare", action="store_true", help="Prepare environment and load test data")
    parser.add_argument("--basic", action="store_true", help="Run basic analytics")
    parser.add_argument("--advanced", action="store_true", help="Run advanced analytics")
    parser.add_argument("--benchmark", action="store_true", help="Run performance benchmark")
    parser.add_argument("--all", action="store_true", help="Run all demos")
    
    args = parser.parse_args()
    
    # Default to --all if no arguments provided
    if not (args.prepare or args.basic or args.advanced or args.benchmark):
        args.all = True
    
    print("🏦 HDBank Data Platform Integration Demo")
    print("=" * 50)
    
    # Set up environment
    setup_environment()
    check_hdfs_status()
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Prepare data
        if args.all or args.prepare:
            print("\n📊 Preparing test data...")
            dataframes = create_test_data(spark)
            save_to_postgres(spark, dataframes)
            save_to_hdfs(dataframes)
        
        # Run basic analytics
        if args.all or args.basic:
            run_basic_analytics(spark)
        
        # Run advanced analytics
        if args.all or args.advanced:
            run_advanced_analytics(spark)
        
        # Run performance benchmark
        if args.all or args.benchmark:
            if 'dataframes' not in locals():
                dataframes = create_test_data(spark)
            compare_hdfs_postgres_performance(spark, dataframes)
        
        print("\n🎉 HDBank Data Platform Demo completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Error occurred: {str(e)}")
        import traceback
        traceback.print_exc()
        
    finally:
        spark.stop()
        print("\n🔌 Spark session stopped.")

if __name__ == "__main__":
    main()
