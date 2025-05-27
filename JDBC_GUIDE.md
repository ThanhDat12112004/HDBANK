# Hướng Dẫn Sử Dụng Demo PySpark-PostgreSQL JDBC

Tài liệu này hướng dẫn cách chạy và sử dụng các chương trình demo kết nối PySpark với PostgreSQL thông qua JDBC trong dự án HDBank PySpark.

## Yêu Cầu Tiên Quyết

1. PostgreSQL đã được cài đặt và đang chạy trên cổng 5432
2. Database `hdbank_db` đã được tạo trong PostgreSQL
3. Tài khoản PostgreSQL có quyền tạo bảng và thực hiện các thao tác CRUD
4. PySpark và Java đã được cài đặt
5. Driver JDBC PostgreSQL đã được tải về (`postgresql-42.7.2.jar`)

## Hướng Dẫn Thiết Lập

### 1. Kiểm tra PostgreSQL

Đảm bảo PostgreSQL đang chạy:

```bash
sudo service postgresql status
```

Nếu PostgreSQL chưa chạy, khởi động nó:

```bash
sudo service postgresql start
```

### 2. Tạo Database (nếu chưa có)

```bash
sudo -u postgres psql -c "CREATE DATABASE hdbank_db;"
```

### 3. Thiết lập người dùng và mật khẩu (nếu cần)

```bash
sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'hdbank123';"
```

### 4. Kiểm tra file JDBC Driver

Đảm bảo file `postgresql-42.7.2.jar` đã có trong thư mục dự án. Nếu chưa có, tải về:

```bash
wget https://jdbc.postgresql.org/download/postgresql-42.7.2.jar -P /home/devduongthanhdat/Desktop/hdbank/hdbank_pyspark/
```

## Các File Demo JDBC

Dự án có hai file demo JDBC:

1. **JDBC.py**: Demo đầy đủ với nhiều tính năng nâng cao
2. **content/simple_jdbc.py**: Demo đơn giản, dễ hiểu cho người mới

## Hướng Dẫn Chạy Demo

### Chạy Demo Đơn Giản

```bash
cd /home/devduongthanhdat/Desktop/hdbank/hdbank_pyspark/
python content/simple_jdbc.py
```

### Chạy Demo Đầy Đủ

```bash
cd /home/devduongthanhdat/Desktop/hdbank/hdbank_pyspark/
python JDBC.py
```

### Các Tùy Chọn Khi Chạy Demo Đầy Đủ

Demo đầy đủ hỗ trợ một số tùy chọn thông qua tham số dòng lệnh:

```bash
python JDBC.py --jar /đường/dẫn/đến/postgresql-42.7.2.jar --db tên_database --user tên_người_dùng --password mật_khẩu
```

- `--jar`: Đường dẫn tới PostgreSQL JDBC JAR (mặc định: /home/devduongthanhdat/Desktop/hdbank/hdbank_pyspark/postgresql-42.7.2.jar)
- `--db`: Tên database PostgreSQL (mặc định: hdbank_db)
- `--user`: Username PostgreSQL (mặc định: postgres)
- `--password`: Mật khẩu PostgreSQL (mặc định: hdbank123)

## Giải Thích Tính Năng Demo Đầy Đủ (JDBC.py)

Demo đầy đủ trong file JDBC.py bao gồm các tính năng:

1. **Tạo Dữ Liệu Mẫu**: Tạo hai bảng `customers` và `transactions` với dữ liệu mẫu.

2. **Đọc Dữ Liệu**: Đọc dữ liệu từ PostgreSQL và hiển thị schema và nội dung.

3. **Thực Thi SQL**: Thực thi các truy vấn SQL phức tạp và lưu kết quả vào PostgreSQL.

4. **Cập Nhật Dữ Liệu**: Demo thao tác cập nhật dữ liệu trong PostgreSQL.

5. **Join Dữ Liệu**: Demo thao tác join giữa các bảng để phân tích dữ liệu.

6. **Partitioning**: Demo kỹ thuật phân vùng dữ liệu để tối ưu hiệu suất.

7. **Pushdown Filter**: Demo tối ưu truy vấn bằng cách đẩy filter xuống PostgreSQL.

## Tích Hợp với HDFS

Để tích hợp demo JDBC với HDFS như đã triển khai trong các phần trước, bạn có thể thực hiện các bước sau:

1. Sử dụng `hdfs_postgresql_integration.py` để xử lý luồng dữ liệu giữa PostgreSQL và HDFS.

2. Chạy `hdbank_data_platform.py` để có trải nghiệm đầy đủ của nền tảng dữ liệu tích hợp.

## Gỡ Lỗi Thông Thường

1. **Lỗi kết nối PostgreSQL**:
   - Kiểm tra PostgreSQL có đang chạy không
   - Kiểm tra thông tin đăng nhập (username/password)
   - Kiểm tra database tồn tại

2. **Lỗi không tìm thấy JDBC driver**:
   - Kiểm tra đường dẫn JAR chính xác
   - Đảm bảo file JAR có quyền đọc

3. **Lỗi về quyền truy cập PostgreSQL**:
   - Kiểm tra người dùng PostgreSQL có quyền tạo bảng không

## Kết Luận

Demo PySpark-PostgreSQL JDBC này cung cấp các ví dụ về cách tích hợp PySpark với hệ quản trị cơ sở dữ liệu PostgreSQL. Kết hợp với các tính năng HDFS đã triển khai trước đó, bạn có thể xây dựng một hệ thống xử lý dữ liệu phân tán toàn diện cho HDBank.
