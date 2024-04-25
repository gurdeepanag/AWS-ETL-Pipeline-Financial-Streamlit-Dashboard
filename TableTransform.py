import pandas as pd
import argparse
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine
import json
import boto3

parser = argparse.ArgumentParser()
parser.add_argument("bucket_name")                          # Source & destination bucket name
parser.add_argument("transformed_file_location")            # Key for the source
parser.add_argument("db_config_json")                       # Key for the JSON file containing db configuration
args = parser.parse_args()

try:
    # Obtain the configurational details
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=args.bucket_name, Key=args.db_config_json)
    db_info = dict(json.loads(response['Body'].read().decode('utf-8')))
    db_url = URL.create(**db_info)
    print("Configuration details provided correctly")

    # Connect to your postgres DB
    engine = create_engine(db_url)
  
    # Execute a query
    pd.read_parquet(f"s3://{args.bucket_name}/{args.transformed_file_location}/company_info").to_sql("company_info", con=engine, if_exists="replace", index=False)
    pd.read_parquet(f"s3://{args.bucket_name}/{args.transformed_file_location}/stock_price").to_sql("stock_price", con=engine, if_exists="replace", index=False)
    pd.read_parquet(f"s3://{args.bucket_name}/{args.transformed_file_location}/financial_statements").to_sql("financial_statements", con=engine, if_exists="replace", index=False)
    pd.read_parquet(f"s3://{args.bucket_name}/{args.transformed_file_location}/ratios").to_sql("ratios", con=engine, if_exists="replace", index=False)

    print("All the files have been pushed to the database")
    print("Step 3 Completed")

except Exception as e:
    print(f"Error while executing Load step: {e}")
