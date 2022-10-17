import boto3, io, gzip

file_name = "wet.paths.gz"
url1 = f"crawl-data/CC-MAIN-2022-05/{file_name}"
bucket_s3 = "commoncrawl"

def downloader(bucket_s3, file_s3):
    s3_client = boto3.client("s3", region_name="eu-north-1")
    f_obj = io.BytesIO()
    s3_client.download_fileobj(
        bucket_s3, file_s3, f_obj
    )
    f_obj.seek(0)
    return f_obj

def extract_url(f_obj):
    with gzip.GzipFile(fileobj=f_obj) as f:
        url = f.readline().decode("utf-8").strip()
    return url


def main():
    f_obj1 = downloader(bucket_s3=bucket_s3, file_s3=url1)
    url2 = extract_url(f_obj=f_obj1)
    f_obj2 = downloader(bucket_s3=bucket_s3, file_s3=url2)
    
    with gzip.GzipFile(fileobj=f_obj2) as f:
        content = f.read().decode("utf-8").splitlines()
        for x in content[:10]:
            print(x)


if __name__ == '__main__':
    main()
