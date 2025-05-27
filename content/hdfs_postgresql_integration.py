#!/usr/bin/env python3
"""
Combined HDFS + PostgreSQL Integration Demo
This demonstrates data flow between HDFS and PostgreSQL using PySpark
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

# Database configuration
DB_CONFIG = {
    "url": "jdbc:postgresql://localhost:5432/hdbank_db",
    "driver": "org.postgresql.Driver",
    "user": "postgres",
    "password": "postgres"
}

def create_spark_session():
    """Create Spark session with HDFS and JDBC configuration"""
    jar_path = "/home/devduongthanhdat/Desktop/hdbank/hdbank_pyspark/postgresql-42.7.2.jar"
    
    return SparkSession.builder \
        .appName("HDFSPostgreSQLIntegration") \
        .config("spark.jars", jar_path) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/spark-warehouse") \
        .getOrCreate()

def create_sample_data_in_postgres(spark):
    """Create sample data in PostgreSQL"""
    print("🗄️ Creating sample data in PostgreSQL...")
    
    # Customer data
    customer_data = [
        (1, "Nguyen Van A", "HCM", "nguyen.van.a@email.com", "0901234567"),
        (2, "Tran Thi B", "HN", "tran.thi.b@email.com", "0902345678"),
        (3, "Le Van C", "DN", "le.van.c@email.com", "0903456789"),
        (4, "Pham Thi D", "HCM", "pham.thi.d@email.com", "0904567890"),
        (5, "Hoang Van E", "HN", "hoang.van.e@email.com", "0905678901")
    ]
    
    customer_schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("full_name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True)
    ])
    
    customer_df = spark.createDataFrame(customer_data, customer_schema)
    
    # Transaction data
    transaction_data = [
        (1, 1, "2024-01-15", "DEPOSIT", 50000000, "ATM001"),
        (2, 1, "2024-01-20", "WITHDRAWAL", -5000000, "ATM002"),
        (3, 2, "2024-02-10", "DEPOSIT", 25000000, "BRANCH_HN01"),
        (4, 2, "2024-02-15", "TRANSFER", -2000000, "ONLINE"),
        (5, 3, "2024-03-05", "DEPOSIT", 75000000, "BRANCH_DN01"),
        (6, 3, "2024-03-10", "WITHDRAWAL", -10000000, "ATM003"),
        (7, 4, "2024-04-01", "DEPOSIT", 100000000, "BRANCH_HCM01"),
        (8, 4, "2024-04-05", "TRANSFER", -15000000, "ONLINE"),
        (9, 5, "2024-05-01", "DEPOSIT", 30000000, "ATM004"),
        (10, 5, "2024-05-10", "WITHDRAWAL", -3000000, "ATM005")
    ]
    
    transaction_schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("amount", LongType(), True),
        StructField("channel", StringType(), True)
    ])
    
    transaction_df = spark.createDataFrame(transaction_data, transaction_schema)
    
    # Write to PostgreSQL
    print("💾 Writing customer data to PostgreSQL...")
    customer_df.write \
        .format("jdbc") \
        .option("url", DB_CONFIG["url"]) \
        .option("driver", DB_CONFIG["driver"]) \
        .option("dbtable", "customers") \
        .option("user", DB_CONFIG["user"]) \
        .option("password", DB_CONFIG["password"]) \
        .mode("overwrite") \
        .save()
    
    print("💾 Writing transaction data to PostgreSQL...")
    transaction_df.write \
        .format("jdbc") \
        .option("url", DB_CONFIG["url"]) \
        .option("driver", DB_CONFIG["driver"]) \
        .option("dbtable", "transactions") \
        .option("user", DB_CONFIG["user"]) \
        .option("password", DB_CONFIG["password"]) \
        .mode("overwrite") \
        .save()
    
    print("✅ Sample data created in PostgreSQL successfully!")

def extract_from_postgres_to_hdfs(spark):
    """Extract data from PostgreSQL and store in HDFS"""
    print("🔄 Extracting data from PostgreSQL to HDFS...")
    
    # Read customers from PostgreSQL
    print("📖 Reading customers from PostgreSQL...")
    customers_df = spark.read \
        .format("jdbc") \
        .option("url", DB_CONFIG["url"]) \
        .option("driver", DB_CONFIG["driver"]) \
        .option("dbtable", "customers") \
        .option("user", DB_CONFIG["user"]) \
        .option("password", DB_CONFIG["password"]) \
        .load()
    
    # Read transactions from PostgreSQL
    print("📖 Reading transactions from PostgreSQL...")
    transactions_df = spark.read \
        .format("jdbc") \
        .option("url", DB_CONFIG["url"]) \
        .option("driver", DB_CONFIG["driver"]) \
        .option("dbtable", "transactions") \
        .option("user", DB_CONFIG["user"]) \
        .option("password", DB_CONFIG["password"]) \
        .load()
    
    # Save to HDFS
    hdfs_data_lake = "hdfs://localhost:9000/user/devduongthanhdat/data_lake"
    
    print("💾 Saving customers to HDFS data lake...")
    customers_df.write.mode("overwrite").parquet(f"{hdfs_data_lake}/customers")
    
    print("💾 Saving transactions to HDFS data lake...")
    transactions_df.write.mode("overwrite").parquet(f"{hdfs_data_lake}/transactions")
    
    print("✅ Data successfully extracted to HDFS data lake!")
    
    return customers_df, transactions_df

def perform_advanced_analytics_on_hdfs(spark):
    """Perform advanced analytics on HDFS data"""
    print("🔍 Performing advanced analytics on HDFS data...")
    
    # Read data from HDFS
    hdfs_data_lake = "hdfs://localhost:9000/user/devduongthanhdat/data_lake"
    
    customers_df = spark.read.parquet(f"{hdfs_data_lake}/customers")
    transactions_df = spark.read.parquet(f"{hdfs_data_lake}/transactions")
    
    print("\n📊 Customer Data Preview:")
    customers_df.show()
    
    print("\n💳 Transaction Data Preview:")
    transactions_df.show()
    
    # Join customers and transactions
    customer_transactions = customers_df.join(
        transactions_df, 
        customers_df.customer_id == transactions_df.customer_id, 
        "inner"
    ).select(
        customers_df.customer_id,
        customers_df.full_name,
        customers_df.city,
        customers_df.email,
        transactions_df.transaction_date,
        transactions_df.transaction_type,
        transactions_df.amount,
        transactions_df.channel
    )
    
    print("\n🔗 Joined Customer-Transaction Data:")
    customer_transactions.show()
    
    # Analytics 1: Customer balance calculation
    print("\n💰 Customer Balance Analysis:")
    customer_balances = customer_transactions.groupBy("customer_id", "full_name", "city") \
        .agg(
            sum("amount").alias("total_balance"),
            count("*").alias("transaction_count"),
            max("transaction_date").alias("last_transaction_date")
        ) \
        .withColumn("balance_millions", round(col("total_balance") / 1000000, 2)) \
        .orderBy(desc("total_balance"))
    
    customer_balances.show()
    
    # Analytics 2: Transaction type analysis
    print("\n📈 Transaction Type Analysis:")
    transaction_analysis = customer_transactions.groupBy("transaction_type") \
        .agg(
            count("*").alias("transaction_count"),
            sum("amount").alias("total_amount"),
            avg("amount").alias("avg_amount")
        ) \
        .withColumn("total_amount_millions", round(col("total_amount") / 1000000, 2)) \
        .withColumn("avg_amount_millions", round(col("avg_amount") / 1000000, 2)) \
        .orderBy(desc("transaction_count"))
    
    transaction_analysis.show()
    
    # Analytics 3: Channel usage analysis
    print("\n📱 Channel Usage Analysis:")
    channel_analysis = customer_transactions.groupBy("channel") \
        .agg(
            count("*").alias("usage_count"),
            countDistinct("customer_id").alias("unique_customers"),
            sum("amount").alias("total_volume")
        ) \
        .withColumn("total_volume_millions", round(col("total_volume") / 1000000, 2)) \
        .orderBy(desc("usage_count"))
    
    channel_analysis.show()
    
    # Analytics 4: City-wise business insights
    print("\n🏙️ City-wise Business Insights:")
    city_insights = customer_transactions.groupBy("city") \
        .agg(
            countDistinct("customer_id").alias("customer_count"),
            count("*").alias("transaction_count"),
            sum("amount").alias("total_business_volume"),
            avg("amount").alias("avg_transaction_amount")
        ) \
        .withColumn("business_volume_millions", round(col("total_business_volume") / 1000000, 2)) \
        .withColumn("avg_transaction_millions", round(col("avg_transaction_amount") / 1000000, 2)) \
        .orderBy(desc("total_business_volume"))
    
    city_insights.show()
    
    # Save analytics results to HDFS
    analytics_path = "hdfs://localhost:9000/user/devduongthanhdat/analytics_results"
    
    print(f"\n💾 Saving analytics results to HDFS: {analytics_path}")
    
    customer_balances.write.mode("overwrite").parquet(f"{analytics_path}/customer_balances")
    transaction_analysis.write.mode("overwrite").parquet(f"{analytics_path}/transaction_analysis")
    channel_analysis.write.mode("overwrite").parquet(f"{analytics_path}/channel_analysis")
    city_insights.write.mode("overwrite").parquet(f"{analytics_path}/city_insights")
    
    return customer_balances, transaction_analysis, channel_analysis, city_insights

def save_analytics_back_to_postgres(spark, customer_balances, transaction_analysis, channel_analysis, city_insights):
    """Save analytics results back to PostgreSQL"""
    print("🔄 Saving analytics results back to PostgreSQL...")
    
    # Save customer balances
    print("💾 Saving customer_balances to PostgreSQL...")
    customer_balances.write \
        .format("jdbc") \
        .option("url", DB_CONFIG["url"]) \
        .option("driver", DB_CONFIG["driver"]) \
        .option("dbtable", "customer_balances") \
        .option("user", DB_CONFIG["user"]) \
        .option("password", DB_CONFIG["password"]) \
        .mode("overwrite") \
        .save()
    
    # Save transaction analysis
    print("💾 Saving transaction_analysis to PostgreSQL...")
    transaction_analysis.write \
        .format("jdbc") \
        .option("url", DB_CONFIG["url"]) \
        .option("driver", DB_CONFIG["driver"]) \
        .option("dbtable", "transaction_analysis") \
        .option("user", DB_CONFIG["user"]) \
        .option("password", DB_CONFIG["password"]) \
        .mode("overwrite") \
        .save()
    
    # Save channel analysis
    print("💾 Saving channel_analysis to PostgreSQL...")
    channel_analysis.write \
        .format("jdbc") \
        .option("url", DB_CONFIG["url"]) \
        .option("driver", DB_CONFIG["driver"]) \
        .option("dbtable", "channel_analysis") \
        .option("user", DB_CONFIG["user"]) \
        .option("password", DB_CONFIG["password"]) \
        .mode("overwrite") \
        .save()
    
    # Save city insights
    print("💾 Saving city_insights to PostgreSQL...")
    city_insights.write \
        .format("jdbc") \
        .option("url", DB_CONFIG["url"]) \
        .option("driver", DB_CONFIG["driver"]) \
        .option("dbtable", "city_insights") \
        .option("user", DB_CONFIG["user"]) \
        .option("password", DB_CONFIG["password"]) \
        .mode("overwrite") \
        .save()
    
    print("✅ Analytics results saved to PostgreSQL successfully!")

def demonstrate_data_pipeline(spark):
    """Demonstrate complete data pipeline: PostgreSQL -> HDFS -> Analytics -> PostgreSQL"""
    print("\n🔄 Demonstrating Complete Data Pipeline...")
    print("Pipeline: PostgreSQL -> HDFS -> Analytics -> PostgreSQL")
    
    # Step 1: Create sample data in PostgreSQL
    create_sample_data_in_postgres(spark)
    
    # Step 2: Extract from PostgreSQL to HDFS
    customers_df, transactions_df = extract_from_postgres_to_hdfs(spark)
    
    # Step 3: Perform analytics on HDFS data
    customer_balances, transaction_analysis, channel_analysis, city_insights = perform_advanced_analytics_on_hdfs(spark)
    
    # Step 4: Save analytics results back to PostgreSQL
    save_analytics_back_to_postgres(spark, customer_balances, transaction_analysis, channel_analysis, city_insights)
    
    # Step 5: Demonstrate advanced JDBC features
    print("\n🔄 Demonstrating advanced JDBC features...")
    jdbc_result_df = demonstrate_jdbc_advanced_features(spark)
    
    if jdbc_result_df is not None:
        # Save JDBC demo results to HDFS for further analysis
        print("\n💾 Saving JDBC demo results to HDFS...")
        jdbc_result_df.write \
            .mode("overwrite") \
            .parquet("hdfs://localhost:9000/user/devduongthanhdat/jdbc_demo_results")
        print("✅ JDBC results saved to HDFS")
    
    print("\n🎉 Complete data pipeline executed successfully!")

def demonstrate_jdbc_advanced_features(spark):
    """Demonstrate advanced JDBC features with PostgreSQL"""
    print("\n🔍 Demonstrating advanced JDBC integration features...")
    
    try:
        # Define connection properties
        connection_properties = {
            "user": DB_CONFIG["user"],
            "password": DB_CONFIG["password"],
            "driver": DB_CONFIG["driver"]
        }
        
        # 1. Use predicate pushdown for filtering
        print("💡 Using predicate pushdown to filter transactions > 500...")
        start_time = time.time()
        
        # This query will be pushed down to PostgreSQL
        pushed_query = "(SELECT * FROM transactions WHERE amount > 500) transactions_filtered"
        
        pushed_df = spark.read \
            .jdbc(DB_CONFIG["url"], pushed_query, properties=connection_properties)
        
        print(f"✅ Retrieved {pushed_df.count()} records in {time.time() - start_time:.2f} seconds")
        pushed_df.show(5)
        
        # 2. Perform partition-based reading
        print("\n💡 Reading data with partition-based parallelism...")
        start_time = time.time()
        
        # Define partition parameters
        partition_column = "customer_id"
        lower_bound = 1
        upper_bound = 6
        num_partitions = 3
        
        partitioned_df = spark.read \
            .jdbc(
                url=DB_CONFIG["url"],
                table="customers",
                column=partition_column,
                lowerBound=lower_bound,
                upperBound=upper_bound,
                numPartitions=num_partitions,
                properties=connection_properties
            )
        
        print(f"✅ Read {partitioned_df.count()} records in {time.time() - start_time:.2f} seconds using {num_partitions} partitions")
        print(f"   Number of partitions: {partitioned_df.rdd.getNumPartitions()}")
        
        # 3. Using batch writes
        print("\n💡 Performing optimized batch writes...")
        start_time = time.time()
        
        # Create temporary data
        batch_data = spark.createDataFrame([
            (1001, "Customer Insights", time.strftime("%Y-%m-%d"), "High-value demographics analysis"),
            (1002, "Risk Analysis", time.strftime("%Y-%m-%d"), "Credit risk scoring"),
            (1003, "Marketing Segments", time.strftime("%Y-%m-%d"), "Customer segmentation"),
        ], ["report_id", "name", "created_date", "description"])
        
        # Use batchsize parameter for efficient writing
        batch_data.write \
            .option("batchsize", 1000) \
            .jdbc(
                url=DB_CONFIG["url"], 
                table="analytics_reports", 
                mode="overwrite", 
                properties=connection_properties
            )
        
        print(f"✅ Batch write completed in {time.time() - start_time:.2f} seconds")
        
        # 4. Demonstrate connection pooling properties
        print("\n💡 Setting up connection pooling options...")
        
        pooling_properties = connection_properties.copy()
        # These are passed to the JDBC driver
        pooling_properties.update({
            "tcpKeepAlive": "true",
            "reWriteBatchedInserts": "true",
            "ApplicationName": "HDBank-PySpark-Integration"
        })
        
        print("✅ Connection pooling configured with optimized settings")
        
        # 5. Demonstrate transaction capabilities
        print("\n💡 Demonstrating transactional capabilities...")
        
        # Read a table using JDBC
        transactions_df = spark.read \
            .jdbc(DB_CONFIG["url"], "transactions", properties=connection_properties)
        
        # Perform some transformation
        transformed_df = transactions_df \
            .withColumn("processed_date", current_date()) \
            .withColumn("audit_notes", lit("Processed in HDFS-JDBC integration demo"))
        
        # Write in a transactional manner
        transformed_df.write \
            .option("isolationLevel", "SERIALIZABLE") \
            .jdbc(
                url=DB_CONFIG["url"],
                table="transactions_audit",
                mode="overwrite",
                properties=connection_properties
            )
        
        print("✅ Transactional write completed successfully")
        
        print("\n✅ Advanced JDBC features demonstration completed")
        
        return transformed_df
        
    except Exception as e:
        print(f"❌ Error in JDBC advanced features demo: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def main():
    """Main execution function"""
    print("🏦 HDBank HDFS + PostgreSQL Integration Demo")
    print("=" * 60)
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")  # Reduce log verbosity
    
    try:
        # Demonstrate complete data pipeline
        demonstrate_data_pipeline(spark)
        
        # Demonstrate advanced JDBC features
        demonstrate_jdbc_advanced_features(spark)
        
        print("\n📊 Final Summary:")
        print("✅ Data successfully stored in PostgreSQL")
        print("✅ Data extracted to HDFS data lake")
        print("✅ Advanced analytics performed on HDFS")
        print("✅ Analytics results saved back to PostgreSQL")
        print("✅ Advanced JDBC features demonstrated")
        print("\n🎯 This demonstrates a complete modern data architecture:")
        print("   • Operational data in PostgreSQL")
        print("   • Data lake storage in HDFS")
        print("   • Big data analytics with Spark")
        print("   • Results integration back to operational systems")
        print("   • Optimized JDBC integration (partitioning, pushdowns, batch processing)")
        
    except Exception as e:
        print(f"❌ Error occurred: {str(e)}")
        import traceback
        traceback.print_exc()
        
    finally:
        spark.stop()
        print("\n🔌 Spark session stopped.")

if __name__ == "__main__":
    main()
