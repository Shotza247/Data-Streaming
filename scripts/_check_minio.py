import boto3
from botocore.config import Config
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)
buckets = s3.list_buckets().get("Buckets", [])
print("MinIO buckets:", [b["Name"] for b in buckets])
for b in buckets:
    resp = s3.list_objects_v2(Bucket=b["Name"], MaxKeys=5)
    cnt  = resp.get("KeyCount", 0)
    keys = [o["Key"] for o in resp.get("Contents", [])]
    print(f"  s3://{b['Name']}: {cnt} objects  {keys}")
