# todo: implement Class structure
# todo: header

# Standard ------------
from io import StringIO
from pathlib import Path

# 3rd Party-
import boto3
import pandas as pd
from botocore.exceptions import ClientError

# Project ----------------------------------------------------
from config.config import AWS_REGION, BUCKET_NAME, S3_RAW_PATH
from config.aws_config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

# Define project root directory
project_root = Path(__file__).resolve().parents[1]

# Define the input XLS and the output CSV file path
# todo: consider adding to config file
input_file = project_root / "data" / "mlb_standings_9242025.xls"  # source provides HTML with xls extension
csv_file = project_root / "data" / "mlb_standings.csv"

# todo: convert to log
if not input_file.exists():
    print(f"Error: Excel file not found at: {input_file}")
    raise FileNotFoundError(f"Missing Excel file: {input_file}")\

# Read input_file into DataFrame, convert to CSV
try:
    # todo: after PoC create version that checks HTML/XLS if data source updates downloads or if a linux version issue.
    #  Implement a file type check and separate them into methods respectively.
    # Read the HTML content from the file
    with open(input_file, 'r', encoding='utf-8') as f:
        html_content = f.read()

    # Parse the HTML tables
    tables = pd.read_html(StringIO(html_content))
    if not tables or all(table.empty for table in tables):
        raise ValueError("No valid tables found in the HTML file")

    # Use the first non-empty table
    df = next(table for table in tables if not table.empty)
    df = df.reset_index(drop=True)

    # Save to CSV
    df.to_csv(csv_file, index=False)
    # todo: convert to log
    print(f"Successfully converted {input_file} to {csv_file}")
    print(f"DataFrame shape: {df.shape} (rows, columns)")

except Exception as e:
    # todo: convert to log
    print(f"Error converting Excel to CSV: {e}")
    raise

# todo: convert to log
print(f"{input_file} converted to {csv_file} successfully.")

# Upload CSV to S3
try:
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    s3_client.upload_file(str(csv_file), BUCKET_NAME, S3_RAW_PATH)
    # todo: convert to log
    print(f"Uploaded {csv_file} to s3://{BUCKET_NAME}/{S3_RAW_PATH}")
except ClientError as e:
    # todo: convert to log
    print(f"Error uploading to S3: {e}")
    raise

