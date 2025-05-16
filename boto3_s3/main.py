import boto3
from botocore.exceptions import ClientError

# Khởi tạo client S3
s3_client = boto3.client('s3', region_name='us-east-1')

# Tên bucket của bạn
bucket_name = 'myawsbucket16052025'

# 1. Liệt kê các tệp trong bucket
try:
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    if 'Contents' in response:
        print(f"Các tệp trong bucket {bucket_name}:")
        for obj in response['Contents']:
            print(f"- {obj['Key']} (Kích thước: {obj['Size']} bytes)")
    else:
        print(f"Bucket {bucket_name} trống.")
except ClientError as e:
    print(f"Lỗi khi liệt kê: {e}")

# 2. Tải tệp lên bucket
def upload_file(file_path, object_name=None):
    if object_name is None:
        object_name = file_path.split('/')[-1]
    try:
        s3_client.upload_file(file_path, bucket_name, object_name)
        print(f"Đã tải lên {file_path} tới {bucket_name}/{object_name}")
    except ClientError as e:
        print(f"Lỗi khi tải lên: {e}")

# 3. Tải tệp từ bucket
def download_file(object_name, file_path):
    try:
        s3_client.download_file(bucket_name, object_name, file_path)
        print(f"Đã tải {object_name} từ {bucket_name} về {file_path}")
    except ClientError as e:
        print(f"Lỗi khi tải xuống: {e}")

# Ví dụ sử dụng
# Tải tệp lên
upload_file('./file.txt')  # Thay bằng đường dẫn tệp thực tế

# Tải tệp xuống
download_file('file.txt', './file.txt')  # Thay bằng tên tệp trên S3 và đường dẫn lưu