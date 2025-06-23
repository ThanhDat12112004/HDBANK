# Script PowerShell: connect_deploy.ps1
# Tự động deploy toàn bộ website lên EC2 instance

$KEY = "instance_mywebserver.pem"
$USER = "ec2-user"
$EC2HOST = "ec2-54-80-204-233.compute-1.amazonaws.com"

# 1. Nén thư mục www
Compress-Archive -Path .\www\* -DestinationPath .\www.zip -Force

# 2. Copy file zip lên EC2
scp -i $KEY .\www.zip "${USER}@${EC2HOST}:/tmp/www.zip"

# 3. SSH vào EC2, giải nén và deploy
$deployCmd = "sudo dnf install -y unzip httpd; " +
    "sudo systemctl enable --now httpd; " +
    "sudo rm -rf /var/www/html/*; " +
    "sudo unzip -o /tmp/www.zip -d /var/www/html; " +
    "sudo rm -f /tmp/www.zip; " +
    "sudo chown -R apache:apache /var/www; " +
    "sudo chmod -R 755 /var/www;"
ssh -i $KEY "${USER}@${EC2HOST}" $deployCmd

# 4. Xóa file zip local
Remove-Item .\www.zip

Write-Host "Deploy thành công! Truy cập: http://${EC2HOST}/"
