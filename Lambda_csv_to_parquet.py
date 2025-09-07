import boto3
import pandas as pd
import urllib.parse
import io
import os

s3 = boto3.client("s3")

def lambda_handler(event, context):
    # 1. Get source bucket & object details from event
    source_bucket = event["Records"][0]["s3"]["bucket"]["name"]
    source_key = urllib.parse.unquote_plus(event["Records"][0]["s3"]["object"]["key"])

    # 2. Target bucket from environment variable
    target_bucket = os.environ.get("TARGET_BUCKET", "your-target-bucket")

    # 3. Generate target key (same prefix, .parquet extension)
    if source_key.endswith(".csv"):
        target_key = source_key.rsplit(".", 1)[0] + ".parquet"
    else:
        target_key = source_key + ".parquet"

    # 4. Read CSV into Pandas
    csv_obj = s3.get_object(Bucket=source_bucket, Key=source_key)
    df = pd.read_csv(io.BytesIO(csv_obj["Body"].read()))

    # 5. Convert to Parquet
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", compression="snappy", index=False)

    # 6. Upload Parquet to target bucket with same key path
    s3.put_object(Bucket=target_bucket, Key=target_key, Body=buffer.getvalue())

    return {
        "status": "success",
        "source": f"s3://{source_bucket}/{source_key}",
        "target": f"s3://{target_bucket}/{target_key}",
    }


""""
General configuration

· Timeout — 5Min
· Memory — 1024MB
· Ephemeral Storage — 2048MB

Environment variables

· TARGET_BUCKET = de01-youtubedataanalysis-cleansed-useast1-dev

Layers
Add - AWSSDKPandas-Python38

"""