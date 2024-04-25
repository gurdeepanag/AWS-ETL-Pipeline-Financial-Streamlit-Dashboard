import numpy as np
import pandas as pd
import argparse
import os, shutil
import boto3
import datetime

parser = argparse.ArgumentParser()
parser.add_argument("bucket_name")                          # Source & destination bucket name
parser.add_argument("raw_file_location")                    # Key for the source
parser.add_argument("transform_file_destination")           # Key for the destination 
args = parser.parse_args()
          
print("Starting with Loading Datasets")
info = pd.read_parquet(f"s3://{args.bucket_name}/{args.raw_file_location}/info")
stock_price = pd.read_parquet(f"s3://{args.bucket_name}/{args.raw_file_location}/stock_price")
financial_statements = pd.read_parquet(f"s3://{args.bucket_name}/{args.raw_file_location}/financial_statements")
print("Loaded Datasets")

# Transforming company details
column_mapping = {
    "ticker": "ticker",
    "shortname": "company_nm",
    "website": "website",
    "industry": "industry",
    "longbusinesssummary": "company_info",
    "fulltimeemployees": "full_time_employees"
}
company_info = info[column_mapping.keys()].rename(columns=column_mapping)
company_info.full_time_employees = company_info.full_time_employees.astype(np.float64)

# Transforming stocks details
column_mapping = {
    "ticker": "ticker",
    "month": "month",
    "open": "opening_price",
    "close": "closing_price",
    "high": "month_high",
    "low": "month_low"
}
stock_price = stock_price[column_mapping.keys()].rename(columns=column_mapping)
stock_price[[col for col in stock_price.columns if col not in ["ticker", "month"]]] = stock_price[[col for col in stock_price.columns if col not in ["ticker", "month"]]].astype(np.float64)

# Transforming financial statement metrics
column_mapping = {
    "ticker": "ticker",
    "month": "month",
    "cash_and_cash_equivalents": "cash_and_cash_equivalents",
    "ebitda": "ebitda",
    "net_income": "net_income",
    "net_debt": "net_debt",
    "total_debt": "total_debt",
    "current_assets": "current_assets",
    "current_liabilities": "current_liabilities"
}

financial_statements = financial_statements[column_mapping.keys()].rename(columns=column_mapping)
financial_statements[[col for col in financial_statements.columns if col not in ["ticker", "month"]]] = financial_statements[[col for col in financial_statements.columns if col not in ["ticker", "month"]]].astype(np.float64)
financial_statements["current_ratio"] = financial_statements.current_assets / financial_statements.current_liabilities

# Find the latest month of available records
temp = financial_statements.groupby(by="ticker")["month"].max().rename_axis("ticker").reset_index()
financial_statements = financial_statements.merge(temp, how="inner").drop(columns=["month"])

# Generating all ratios
column_mapping = {
    "ticker": "ticker",
    "sharesoutstanding": "outstanding_shares",
    "previousclose": "latest_closing_price",
    "freecashflow": "free_cash_flow",
    "operatingcashflow": "operating_cash_flow",
    "dividendyield": "dividend_yield",
    "trailingpe": "trailing_pe",
    "debttoequity": "debt_to_equity",
    "returnonassets": "return_on_assets",
    "returnonequity": "return_on_equity",
}

ratios = info[column_mapping.keys()].rename(columns=column_mapping)
ratios.loc[:, ratios.columns != "ticker"] = ratios.loc[:, ratios.columns != "ticker"].astype(np.float64)

ratios["market_cap"] = ratios.outstanding_shares * ratios.latest_closing_price

# Add the EV/EBITDA & Current Ratio to the Ratios table
temp = temp.merge(financial_statements[["total_debt", "cash_and_cash_equivalents", "ebitda", "current_ratio", "ticker"]], how="inner")
temp = temp.merge(ratios[["ticker", "market_cap"]], how="inner")
temp["ev_to_ebitda"] = (temp.market_cap + temp.total_debt - temp.cash_and_cash_equivalents) / temp.ebitda
ratios = ratios.merge(temp[["ticker", "current_ratio", "ev_to_ebitda"]], how="left")

# Creating empty directories where these files will be stored
dump_path = os.path.join(os.getcwd(), "transformed_datasets")
if os.path.isdir(dump_path):
    shutil.rmtree(dump_path)

os.mkdir(dump_path)
os.mkdir(os.path.join(dump_path, "company_info"))
os.mkdir(os.path.join(dump_path, "stock_price"))
os.mkdir(os.path.join(dump_path, "financial_statements"))
os.mkdir(os.path.join(dump_path, "ratios"))

company_info.to_parquet(os.path.join(os.path.join(dump_path, "company_info"), f"part-0.parquet"), index=False)
stock_price.to_parquet(os.path.join(os.path.join(dump_path, "stock_price"), f"part-0.parquet"), index=False)
financial_statements.to_parquet(os.path.join(os.path.join(dump_path, "financial_statements"), f"part-0.parquet"), index=False)
ratios.to_parquet(os.path.join(os.path.join(dump_path, "ratios"), f"part-0.parquet"), index=False)

try:
    s3 = boto3.client('s3')
    # If the directory is already existing, empty it first
    response = s3.list_objects_v2(Bucket=args.bucket_name, Prefix=args.transform_file_destination)
    if response["KeyCount"]:
        for object in response['Contents']:
            s3.delete_object(Bucket=args.bucket_name, Key=object['Key'])

    # Uploading all the files from the source path to specified key on S3
    for root, _, files in os.walk(dump_path):
        for file in files:
            s3.upload_file(os.path.join(root, file), args.bucket_name, os.path.join(root, file).replace(dump_path, args.transform_file_destination).strip("/"))

    print("Files successfully uploaded to S3")
    print("Step 2 Completed")
    with open("transform_step_completed.txt", "w+") as lambda_file:
        lambda_file.write(f"Completed on: {datetime.datetime.now()}")

    s3 = boto3.client('s3')
    s3.upload_file("transform_step_completed.txt", args.bucket_name, f"{args.transform_file_destination}/transform_step_completed.txt")
except Exception as e:
    print(e)
    print("S3 upload was unsuccessful")