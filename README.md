# Hướng dẫn sử dụng EC2 cơ bản

## 1. EC2 là gì?
Amazon EC2 (Elastic Compute Cloud) là dịch vụ máy chủ ảo trên nền tảng đám mây AWS, cho phép bạn tạo, quản lý và vận hành các máy chủ (instance) một cách linh hoạt.

## 2. Các bước sử dụng EC2 cơ bản

### a. Khởi tạo EC2 Instance
1. Đăng nhập AWS Console.
2. Vào EC2 → Launch Instance.
3. Chọn AMI (ví dụ: Amazon Linux 2).
4. Chọn loại máy (t2.micro miễn phí).
5. Thiết lập key pair (tải file `.pem` về máy).
6. Thiết lập security group (mở port 22 cho SSH, 80 cho HTTP).
7. Launch Instance.

### b. Kết nối SSH vào EC2
- Trên máy tính, dùng terminal:
  ```sh
  ssh -i "instance_mywebserver.pem" ec2-user@<EC2-PUBLIC-IP>
  ```
- Lưu ý: file `.pem` phải có quyền 400 (`chmod 400 instance_mywebserver.pem` trên Linux/macOS).

### c. Triển khai website lên EC2
1. Cài đặt web server (Apache/Nginx):
   ```sh
   sudo dnf install -y httpd
   sudo systemctl enable --now httpd
   ```
2. Upload source code lên EC2 (dùng scp hoặc script tự động).
3. Đặt source vào đúng thư mục web server (mặc định: `/var/www/html/` hoặc `/www/`).
4. Truy cập website qua trình duyệt: `http://<EC2-PUBLIC-IP>/`.

### d. Một số lệnh quản lý EC2
- Khởi động lại web server:
  ```sh
  sudo systemctl restart httpd
  ```
- Xem trạng thái web server:
  ```sh
  sudo systemctl status httpd
  ```
- Xem địa chỉ IP public:
  - Trên AWS Console hoặc dùng lệnh: `curl http://169.254.169.254/latest/meta-data/public-ipv4`

## 3. Lưu ý bảo mật
- Không chia sẻ file `.pem` cho người khác.
- Chỉ mở port cần thiết trên security group.
- Thường xuyên cập nhật hệ thống và backup dữ liệu.

---
**Tài liệu này dành cho người mới bắt đầu làm quen với EC2 và triển khai website tĩnh.**
