import boto3
from botocore.exceptions import ClientError

# Khởi tạo client S3
s3_client = boto3.client('s3', region_name='us-east-1')

# Tên bucket của bạn
bucket_name = 'myawsbucket16052025'


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


def upload_image_and_get_url(image_path, object_name=None):
    if object_name is None:
        object_name = image_path.split('/')[-1]
    
    try:
        # Tải ảnh lên với Content-Type phù hợp
        s3_client.upload_file(
            image_path, 
            bucket_name, 
            object_name,
            ExtraArgs={'ContentType': 'image/jpeg'}  # Điều chỉnh Content-Type theo loại ảnh
        )
        
        # Tạo URL công khai
        url = create_presigned_url(object_name)
        print(f"Đã tải ảnh lên thành công. URL công khai: {url}")
        return url
    except ClientError as e:
        print(f"Lỗi khi tải ảnh lên: {e}")
        return None

# 5. Tạo URL có thời hạn cho đối tượng
def create_presigned_url(object_name, expiration=3600):
    try:
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': bucket_name,
                'Key': object_name
            },
            ExpiresIn=expiration
        )
        return url
    except ClientError as e:
        print(f"Lỗi khi tạo URL: {e}")
        return None



# Tải ảnh avatar lên và lấy URL
image_url = upload_image_and_get_url('./avatar.jpeg')
if image_url:
    print(f"Bạn có thể xem ảnh tại URL này: {image_url}")