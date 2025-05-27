# HDBank PySpark Integration with HDFS and PostgreSQL

## Tổng quan

Dự án này trình bày việc tích hợp PySpark với hai nền tảng lưu trữ dữ liệu quan trọng:
1. **HDFS (Hadoop Distributed File System)** - Hệ thống lưu trữ phân tán dùng cho Big Data
2. **PostgreSQL** - Hệ quản trị cơ sở dữ liệu quan hệ mạnh mẽ

Dự án cung cấp các ví dụ thực tế về:
- Đọc/ghi dữ liệu từ PostgreSQL và HDFS
- So sánh hiệu năng giữa các định dạng file khác nhau (CSV, Parquet, JSON)
- Phân tích dữ liệu tài chính với PySpark
- Xây dựng các quy trình ETL đa dạng
- So sánh hiệu năng giữa HDFS và PostgreSQL trong các tác vụ khác nhau

## Yêu cầu hệ thống

- Java 17 hoặc cao hơn
- Python 3.8 hoặc cao hơn
- Apache Hadoop 3.3.6
- PostgreSQL 16
- PySpark 3.5.0 hoặc cao hơn
- JDBC Driver cho PostgreSQL (postgresql-42.7.2.jar)

## Cài đặt

### 1. Cài đặt và cấu hình Hadoop HDFS

```bash
# Giải nén Hadoop
tar -xzf hadoop-3.3.6.tar.gz

# Thiết lập biến môi trường
export HADOOP_HOME=/home/devduongthanhdat/Desktop/hdbank/hadoop-3.3.6
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Cấu hình HDFS
# Chỉnh sửa $HADOOP_HOME/etc/hadoop/hadoop-env.sh, core-site.xml, và hdfs-site.xml
# Đã được thực hiện qua script

# Format NameNode
$HADOOP_HOME/bin/hdfs namenode -format -force

# Khởi động HDFS
$HADOOP_HOME/bin/hdfs --daemon start namenode
$HADOOP_HOME/bin/hdfs --daemon start datanode
```

### 2. Thiết lập môi trường

```bash
# Cấp quyền thực thi cho script thiết lập
chmod +x /home/devduongthanhdat/Desktop/hdbank/setup_hdfs_env.sh

# Chạy script thiết lập
/home/devduongthanhdat/Desktop/hdbank/setup_hdfs_env.sh
```

## Cấu trúc dự án

```
hdbank_pyspark/
├── main.py                     # So sánh hiệu năng CSV và Parquet
├── JDBC.py                     # Demo tích hợp đầy đủ với PostgreSQL JDBC
├── JDBC_GUIDE.md               # Hướng dẫn sử dụng demo JDBC
├── postgresql-42.7.2.jar       # JDBC Driver cho PostgreSQL
├── content/
│   ├── simple_jdbc.py          # Demo đơn giản cho JDBC
│   ├── hdfs_integration.py     # Demo tích hợp với HDFS
│   ├── hdfs_postgresql_integration.py    # Demo kết hợp HDFS và PostgreSQL
│   └── hdbank_data_platform.py           # Nền tảng dữ liệu tích hợp hoàn chỉnh
```

## Các demo có sẵn

### 1. Demo HDFS Integration (`hdfs_integration.py`)

Tích hợp với HDFS để:
- Đọc/ghi dữ liệu với nhiều định dạng khác nhau (Parquet, CSV, JSON)
- So sánh hiệu năng giữa các định dạng
- Phân tích dữ liệu trên HDFS
- So sánh hiệu năng giữa HDFS và hệ thống tệp cục bộ

```bash
cd /home/devduongthanhdat/Desktop/hdbank/hdbank_pyspark
python3 content/hdfs_integration.py
```

### 2. Demo HDFS + PostgreSQL (`hdfs_postgresql_integration.py`)

Kết hợp cả HDFS và PostgreSQL trong một quy trình ETL hoàn chỉnh:
- Tạo dữ liệu mẫu trong PostgreSQL
- Trích xuất dữ liệu từ PostgreSQL đến HDFS
- Thực hiện phân tích nâng cao trên HDFS
- Lưu kết quả phân tích trở lại PostgreSQL

```bash
cd /home/devduongthanhdat/Desktop/hdbank/hdbank_pyspark
python3 content/hdfs_postgresql_integration.py
```

### 3. Nền tảng dữ liệu HDBank (`hdbank_data_platform.py`)

Demo tích hợp toàn diện với nhiều tính năng nâng cao:
- Quản lý môi trường tự động
- Tạo dữ liệu mẫu phong phú cho ngân hàng
- Phân tích cơ bản và nâng cao
- So sánh hiệu năng chi tiết
- Command-line interface linh hoạt

```bash
cd /home/devduongthanhdat/Desktop/hdbank/hdbank_pyspark
python3 content/hdbank_data_platform.py --all        # Chạy tất cả các demo
python3 content/hdbank_data_platform.py --prepare    # Chỉ chuẩn bị dữ liệu
python3 content/hdbank_data_platform.py --basic      # Chạy phân tích cơ bản
python3 content/hdbank_data_platform.py --advanced   # Chạy phân tích nâng cao
python3 content/hdbank_data_platform.py --benchmark  # Chạy đánh giá hiệu năng
```

### 4. Demo PySpark-PostgreSQL JDBC (`JDBC.py`)

Demo toàn diện về tích hợp PySpark với PostgreSQL qua JDBC:
- Kết nối PySpark với PostgreSQL
- Thực hiện đầy đủ các thao tác CRUD (Create, Read, Update, Delete)
- Thực thi các truy vấn SQL phức tạp
- Tối ưu hiệu suất với kỹ thuật partitioning và pushdown filter
- Phân tích dữ liệu ngân hàng với SQL và DataFrame API

```bash
cd /home/devduongthanhdat/Desktop/hdbank/hdbank_pyspark
python3 JDBC.py                                  # Chạy demo đầy đủ
python3 JDBC.py --jar /path/to/jdbc-driver.jar   # Chỉ định JDBC driver khác
python3 JDBC.py --db hdbank_test --user test     # Kết nối đến database khác
```

### 5. Demo JDBC Đơn Giản (`simple_jdbc.py`)

Phiên bản đơn giản của demo JDBC, phù hợp cho người mới bắt đầu:
- Kết nối cơ bản PySpark với PostgreSQL
- Tạo và đọc dữ liệu đơn giản
- Code ngắn gọn, dễ hiểu

```bash
cd /home/devduongthanhdat/Desktop/hdbank/hdbank_pyspark
python3 content/simple_jdbc.py
```

## Web UI

- HDFS NameNode UI: [http://localhost:9870](http://localhost:9870)
- HDFS DataNode UI: [http://localhost:9864](http://localhost:9864)

## Kết quả hiệu năng

Các kết quả so sánh hiệu năng giữa HDFS và PostgreSQL được lưu trữ trong:
- HDFS: `hdfs://localhost:9000/user/devduongthanhdat/analytics_results/performance_comparison`
- Dữ liệu phân tích: `hdfs://localhost:9000/user/devduongthanhdat/analytics_results/`

## Quản lý HDFS

### Kiểm tra trạng thái HDFS
```bash
jps  # Hiển thị các Java processes đang chạy, bao gồm NameNode và DataNode
```

### Dừng HDFS
```bash
$HADOOP_HOME/bin/hdfs --daemon stop namenode
$HADOOP_HOME/bin/hdfs --daemon stop datanode
```

### Khởi động HDFS
```bash
$HADOOP_HOME/bin/hdfs --daemon start namenode
$HADOOP_HOME/bin/hdfs --daemon start datanode
```

### Các lệnh HDFS thường dùng
```bash
# Liệt kê các file/thư mục
$HADOOP_HOME/bin/hdfs dfs -ls /

# Tạo thư mục
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /user/devduongthanhdat/new_dir

# Upload file lên HDFS
$HADOOP_HOME/bin/hdfs dfs -put local_file.txt /user/devduongthanhdat/

# Tải file từ HDFS về local
$HADOOP_HOME/bin/hdfs dfs -get /user/devduongthanhdat/file.txt local_file.txt

# Xóa file/thư mục
$HADOOP_HOME/bin/hdfs dfs -rm -r /user/devduongthanhdat/dir_to_delete
```

## Kết luận

Dự án này cho thấy cách tích hợp PySpark với cả HDFS và PostgreSQL để tạo ra một nền tảng dữ liệu hoàn chỉnh. Bằng cách kết hợp sức mạnh của HDFS cho xử lý big data và PostgreSQL cho lưu trữ quan hệ, chúng ta có thể xây dựng các giải pháp phân tích dữ liệu mạnh mẽ và linh hoạt.

Việc triển khai demo JDBC toàn diện cho PostgreSQL cung cấp các kỹ thuật tối ưu hóa và mô hình xử lý dữ liệu quan hệ hiệu quả. Khi kết hợp với khả năng xử lý dữ liệu phân tán của HDFS, nền tảng này cung cấp một giải pháp hoàn chỉnh cho việc xử lý và phân tích dữ liệu trong môi trường ngân hàng.

### Các bước tiếp theo

1. **Tích hợp với Apache Kafka** để xử lý dữ liệu streaming
2. **Phát triển các mô hình ML** sử dụng MLlib của Spark
3. **Tối ưu hóa hiệu năng** với các kỹ thuật nâng cao như caching và tuning
4. **Triển khai nền tảng** trong môi trường sản xuất với cơ chế giám sát và khôi phục