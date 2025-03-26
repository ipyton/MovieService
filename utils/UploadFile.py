from minio import Minio
from minio.error import S3Error
import os

# MinIO 客户端
minio_client = Minio(
    "localhost:9000",
    access_key="admin",
    secret_key="admin123",
    secure=False
)

bucket_name = "longvideos"  # 你的桶名称
directory_path = "./processed"  # 要上传的文件夹路径

# 确保桶存在


# 上传文件
def upload_files(inputPath, bucket_name, outputPath):
    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
    except S3Error as e:
        print(f"存储桶创建失败: {e}")
        return False
    try:
        for root, _, files in os.walk(inputPath):
            for file in files:
                file_path = os.path.join(root, file)
                object_name = os.path.join(outputPath, file).replace("\\", "/")  # 仅使用文件名
                print(bucket_name)
                print(object_name)
                print(file_path)
                minio_client.fput_object(bucket_name,  object_name, file_path)
                print(f"成功上传: {file_path} -> {bucket_name}/{object_name}",flush=True)

    except Exception as e:
        print(f"上传失败: {file_path}, 错误: {e}", flush=True)
        return False
