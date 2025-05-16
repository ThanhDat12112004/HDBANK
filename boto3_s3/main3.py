import boto3
from botocore.exceptions import ClientError
import os
import mimetypes

# Khởi tạo client S3
s3_client = boto3.client('s3', region_name='us-east-1')

# Thông tin bucket và CloudFront
bucket_name = 'myawsbucket16052025'
cloudfront_domain = 'd19ngcu0481pt.cloudfront.net'

# Hàm upload ảnh lên S3 với Content-Type
def upload_image(file_path, folder='images'):
    if not os.path.exists(file_path):
        print(f"Tệp {file_path} không tồn tại!")
        return None

    # Tạo object key (tên tệp trên S3)
    file_name = os.path.basename(file_path)
    object_key = f"{folder}/{file_name}"

    # Xác định Content-Type dựa trên đuôi tệp
    content_type = mimetypes.guess_type(file_path)[0] or 'application/octet-stream'

    try:
        s3_client.upload_file(
            file_path,
            bucket_name,
            object_key,
            ExtraArgs={'ContentType': content_type}
        )
        print(f"Đã tải lên {file_path} tới {bucket_name}/{object_key}")
        return object_key
    except ClientError as e:
        print(f"Lỗi khi tải lên: {e}")
        return None

# Hàm lấy URL ảnh (đã điều chỉnh cho Origin Path)
def get_image_url(object_key):
    if not object_key:
        return None
    # Vì Origin Path là /images, chỉ lấy phần sau /images trong object_key
    file_name = object_key.split('/', 1)[1] if '/' in object_key else object_key
    cloudfront_url = f"https://{cloudfront_domain}/{file_name}"
    return cloudfront_url

# Sử dụng
def main():
    # Đường dẫn ảnh trên máy
    image_path = './avatar.png'  # Đường dẫn ảnh của bạn

    # Tải ảnh lên S3
    object_key = upload_image(image_path, folder='images')
    
    if object_key:
        # Lấy URL ảnh
        image_url = get_image_url(object_key)
        print(f"URL ảnh: {image_url}")

if __name__ == "__main__":
    main()