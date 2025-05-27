#!/usr/bin/env python3
"""
HDFS Integration with PySpark Demo
This demonstrates how to read/write data from/to HDFS using PySpark
"""

import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Hadoop environment setup
HADOOP_HOME = "/home/devduongthanhdat/Desktop/hdbank/hadoop-3.3.6"
JAVA_HOME = "/usr/lib/jvm/java-17-openjdk-amd64"

# Set environment variables
os.environ["HADOOP_HOME"] = HADOOP_HOME
os.environ["JAVA_HOME"] = JAVA_HOME
os.environ["HADOOP_CONF_DIR"] = f"{HADOOP_HOME}/etc/hadoop"

def create_spark_session():
    """Create Spark session with HDFS configuration"""
    return SparkSession.builder \
        .appName("HDFSIntegrationDemo") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/spark-warehouse") \
        .getOrCreate()

def generate_sample_data(spark):
    """Generate sample financial data"""
    print("📊 Generating sample financial data...")
    
    # Sample data cho HDBank
    data = [
        ("CUST001", "Nguyen Van A", "HCM", "Savings", 50000000, "2024-01-15"),
        ("CUST002", "Tran Thi B", "HN", "Checking", 25000000, "2024-02-20"),
        ("CUST003", "Le Van C", "DN", "Savings", 75000000, "2024-03-10"),
        ("CUST004", "Pham Thi D", "HCM", "Investment", 100000000, "2024-04-05"),
        ("CUST005", "Hoang Van E", "HN", "Savings", 30000000, "2024-05-12"),
        ("CUST006", "Vu Thi F", "CT", "Checking", 45000000, "2024-06-08"),
        ("CUST007", "Dao Van G", "HCM", "Investment", 80000000, "2024-07-22"),
        ("CUST008", "Bui Thi H", "HN", "Savings", 35000000, "2024-08-15"),
        ("CUST009", "Ngo Van I", "DN", "Checking", 60000000, "2024-09-30"),
        ("CUST010", "Dang Thi K", "HCM", "Investment", 120000000, "2024-10-18")
    ]
    
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("account_type", StringType(), True),
        StructField("balance", LongType(), True),
        StructField("open_date", StringType(), True)
    ])
    
    return spark.createDataFrame(data, schema)

def demonstrate_hdfs_operations(spark):
    """Demonstrate HDFS read/write operations"""
    print("🚀 Starting HDFS Integration Demo...")
    
    # Generate sample data
    df = generate_sample_data(spark)
    
    print("\n📈 Sample Data Preview:")
    df.show()
    
    # Write data to HDFS in different formats
    hdfs_base_path = "hdfs://localhost:9000/user/devduongthanhdat/hdbank_data"
    
    print(f"\n💾 Writing data to HDFS: {hdfs_base_path}")
    
    # 1. Write as Parquet (optimal for analytics)
    start_time = time.time()
    parquet_path = f"{hdfs_base_path}/customers.parquet"
    df.write.mode("overwrite").parquet(parquet_path)
    parquet_write_time = time.time() - start_time
    print(f"✅ Parquet write completed in {parquet_write_time:.2f} seconds")
    
    # 2. Write as CSV
    start_time = time.time()
    csv_path = f"{hdfs_base_path}/customers.csv"
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_path)
    csv_write_time = time.time() - start_time
    print(f"✅ CSV write completed in {csv_write_time:.2f} seconds")
    
    # 3. Write as JSON
    start_time = time.time()
    json_path = f"{hdfs_base_path}/customers.json"
    df.write.mode("overwrite").json(json_path)
    json_write_time = time.time() - start_time
    print(f"✅ JSON write completed in {json_write_time:.2f} seconds")
    
    print("\n📊 Write Performance Comparison:")
    print(f"Parquet: {parquet_write_time:.2f}s")
    print(f"CSV: {csv_write_time:.2f}s") 
    print(f"JSON: {json_write_time:.2f}s")
    
    # Read data back from HDFS
    print("\n📖 Reading data back from HDFS...")
    
    # Read Parquet
    start_time = time.time()
    df_parquet = spark.read.parquet(parquet_path)
    parquet_read_time = time.time() - start_time
    print(f"✅ Parquet read completed in {parquet_read_time:.2f} seconds")
    
    # Read CSV
    start_time = time.time()
    df_csv = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
    csv_read_time = time.time() - start_time
    print(f"✅ CSV read completed in {csv_read_time:.2f} seconds")
    
    # Read JSON
    start_time = time.time()
    df_json = spark.read.json(json_path)
    json_read_time = time.time() - start_time
    print(f"✅ JSON read completed in {json_read_time:.2f} seconds")
    
    print("\n📊 Read Performance Comparison:")
    print(f"Parquet: {parquet_read_time:.2f}s")
    print(f"CSV: {csv_read_time:.2f}s")
    print(f"JSON: {json_read_time:.2f}s")
    
    return df_parquet

def perform_analytics_on_hdfs_data(spark, df):
    """Perform analytics on HDFS data"""
    print("\n🔍 Performing Analytics on HDFS Data...")
    
    # Convert balance to millions for better readability
    df_analytics = df.withColumn("balance_millions", round(col("balance") / 1000000, 2))
    
    # 1. Account type distribution
    print("\n💳 Account Type Distribution:")
    account_dist = df_analytics.groupBy("account_type") \
        .agg(
            count("*").alias("customer_count"),
            sum("balance_millions").alias("total_balance_millions"),
            avg("balance_millions").alias("avg_balance_millions")
        ) \
        .orderBy(desc("total_balance_millions"))
    
    account_dist.show()
    
    # 2. City-wise analysis
    print("\n🏙️ City-wise Analysis:")
    city_analysis = df_analytics.groupBy("city") \
        .agg(
            count("*").alias("customer_count"),
            sum("balance_millions").alias("total_balance_millions"),
            avg("balance_millions").alias("avg_balance_millions")
        ) \
        .orderBy(desc("total_balance_millions"))
    
    city_analysis.show()
    
    # 3. High-value customers (>50M VND)
    print("\n💰 High-Value Customers (>50M VND):")
    high_value = df_analytics.filter(col("balance_millions") > 50) \
        .select("customer_name", "city", "account_type", "balance_millions") \
        .orderBy(desc("balance_millions"))
    
    high_value.show()
    
    # Save analytics results back to HDFS
    analytics_path = "hdfs://localhost:9000/user/devduongthanhdat/hdbank_analytics"
    
    print(f"\n💾 Saving analytics results to HDFS: {analytics_path}")
    
    # Save account distribution
    account_dist.write.mode("overwrite").parquet(f"{analytics_path}/account_distribution.parquet")
    
    # Save city analysis
    city_analysis.write.mode("overwrite").parquet(f"{analytics_path}/city_analysis.parquet")
    
    # Save high-value customers
    high_value.write.mode("overwrite").parquet(f"{analytics_path}/high_value_customers.parquet")
    
    print("✅ Analytics results saved successfully!")

def demonstrate_hdfs_vs_local_performance(spark):
    """Compare HDFS vs Local filesystem performance"""
    print("\n⚡ HDFS vs Local Filesystem Performance Comparison...")
    
    # Generate larger dataset for performance testing
    print("📊 Generating larger dataset for performance test...")
    large_data = []
    for i in range(10000):
        large_data.append((
            f"CUST{i:06d}",
            f"Customer {i}",
            ["HCM", "HN", "DN", "CT", "VT"][i % 5],
            ["Savings", "Checking", "Investment"][i % 3],
            (i * 1000 + 10000) * 1000,  # Random balance
            f"2024-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}"
        ))
    
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("account_type", StringType(), True),
        StructField("balance", LongType(), True),
        StructField("open_date", StringType(), True)
    ])
    
    large_df = spark.createDataFrame(large_data, schema)
    
    # Test HDFS write performance
    print("\n💾 Testing HDFS write performance...")
    hdfs_path = "hdfs://localhost:9000/user/devduongthanhdat/performance_test_hdfs.parquet"
    start_time = time.time()
    large_df.write.mode("overwrite").parquet(hdfs_path)
    hdfs_write_time = time.time() - start_time
    print(f"✅ HDFS write: {hdfs_write_time:.2f} seconds")
    
    # Test Local write performance
    print("\n💾 Testing Local write performance...")
    local_path = "/home/devduongthanhdat/Desktop/hdbank/hdbank_pyspark/content/performance_test_local.parquet"
    start_time = time.time()
    large_df.write.mode("overwrite").parquet(local_path)
    local_write_time = time.time() - start_time
    print(f"✅ Local write: {local_write_time:.2f} seconds")
    
    # Test HDFS read performance
    print("\n📖 Testing HDFS read performance...")
    start_time = time.time()
    hdfs_df = spark.read.parquet(hdfs_path)
    hdfs_count = hdfs_df.count()  # Trigger action
    hdfs_read_time = time.time() - start_time
    print(f"✅ HDFS read ({hdfs_count} records): {hdfs_read_time:.2f} seconds")
    
    # Test Local read performance
    print("\n📖 Testing Local read performance...")
    start_time = time.time()
    local_df = spark.read.parquet(local_path)
    local_count = local_df.count()  # Trigger action
    local_read_time = time.time() - start_time
    print(f"✅ Local read ({local_count} records): {local_read_time:.2f} seconds")
    
    # Performance summary
    print("\n📊 Performance Summary:")
    print(f"{'Operation':<15} {'HDFS (s)':<10} {'Local (s)':<10} {'Difference':<15}")
    print("-" * 55)
    print(f"{'Write':<15} {hdfs_write_time:<10.2f} {local_write_time:<10.2f} {(hdfs_write_time/local_write_time):.2f}x")
    print(f"{'Read':<15} {hdfs_read_time:<10.2f} {local_read_time:<10.2f} {(hdfs_read_time/local_read_time):.2f}x")

def main():
    """Main execution function"""
    print("🏦 HDBank PySpark HDFS Integration Demo")
    print("=" * 50)
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")  # Reduce log verbosity
    
    try:
        # Demonstrate basic HDFS operations
        df = demonstrate_hdfs_operations(spark)
        
        # Perform analytics on HDFS data
        perform_analytics_on_hdfs_data(spark, df)
        
        # Performance comparison
        demonstrate_hdfs_vs_local_performance(spark)
        
        print("\n🎉 HDFS Integration Demo completed successfully!")
        
    except Exception as e:
        print(f"❌ Error occurred: {str(e)}")
        import traceback
        traceback.print_exc()
        
    finally:
        spark.stop()
        print("\n🔌 Spark session stopped.")

if __name__ == "__main__":
    main()
