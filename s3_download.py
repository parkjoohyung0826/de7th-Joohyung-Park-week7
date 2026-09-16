import boto3
import csv
import os
import sys


if len(sys.argv) != 2:
    print("Usage: python3 s3_download.py <bucket-name>")
    sys.exit(1)

bucket_name = sys.argv[1]

s3 = boto3.client("s3")

prefix = "bronze/"
key = "bronze/netflix_titles.csv"

local_dir = "data"
local_path = os.path.join(local_dir, "netflix_titles.csv")


# 1. bronze/ 객체 목록과 크기 출력
print(f"[1] S3 objects in s3://{bucket_name}/{prefix}")

response = s3.list_objects_v2(
    Bucket=bucket_name,
    Prefix=prefix,
)

for obj in response.get("Contents", []):
    print(f"{obj['Key']} - {obj['Size']} bytes")


# 2. 파일 다운로드
os.makedirs(local_dir, exist_ok=True)

print(f"\n[2] Downloading s3://{bucket_name}/{key}")

s3.download_file(
    bucket_name,
    key,
    local_path,
)

print(f"Downloaded to: {local_path}")


# 3. CSV 레코드 수 계산 (헤더 제외)
with open(local_path, "r", encoding="utf-8-sig", newline="") as f:
    reader = csv.reader(f)

    next(reader)  # header 제외

    row_count = sum(1 for _ in reader)

print(f"\n[3] CSV record count: {row_count}")
