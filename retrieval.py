import yfinance as yf
import numpy as np
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
import boto3
import os, shutil
import argparse

def getDescription(tickers: list[str]):
    '''
    Input: List of tickers
    Output: DataFrame with instanteous values of each ticker 
    Example: getDescription(["AAPL", "GOOG"])
    '''
    description = []
    for i in tickers:
        ticker = yf.Ticker(i)
        info = pd.Series(ticker.info, name=i).astype("string")
        description.append(pd.Series(info))

    df = pd.concat(description, axis=1).transpose()
    df.columns = list(map(lambda x: ''.join(e for e in x.lower().replace(" ", "_") if e.isalnum() or e == "_"), df.columns))
    df = df.rename_axis("ticker").reset_index()
    return df

def getStock(tickers: list[str], num_years: int, interval: str):
    '''
    Input: List of tickers, look back period in terms of number of years, interval across which the metrics are computed 
    Output: DataFrame with time-series data regarding the stock of each ticker - high, low, close, open, etc.
    Example: getStock(["AAPL", "GOOG"], 2, "1mo") 
    '''
    stocks = []
    for i in tickers:
        ticker = yf.Ticker(i)
        df = ticker.history(start=datetime.datetime.today() - relativedelta(years = num_years), 
                            end=datetime.datetime.today(), 
                            interval=interval
                            )
        df["ticker"] = i
        stocks.append(df)
    
    stocks = pd.concat(stocks)
    stocks.index = list(map(lambda x: datetime.date.strftime(x, "%Y-%m"), stocks.index))
    stocks.columns = list(map(lambda x: ''.join(e for e in x.lower().replace(" ", "_") if e.isalnum() or e == "_"), stocks.columns))
    stocks = stocks.rename_axis("month").reset_index()
    return stocks

def getQuarterlyInformation(tickers):
    '''
    Input: List of tickers
    Output: DataFrame with time-series data obtained from the financial statements for each ticker 
    Example: getQuarterlyInformation(["AAPL", "GOOG"])
    '''
    quarterly_metrics = []
    for i in tickers:
        ticker = yf.Ticker(i)
        df = pd.concat([ticker.quarterly_financials.transpose(), ticker.quarterly_balance_sheet.transpose()], axis=1)
        df["ticker"] = i
        quarterly_metrics.append(df)
    
    quarterly_metrics = pd.concat(quarterly_metrics)
    quarterly_metrics.index = list(map(lambda x: datetime.date.strftime(x, "%Y-%m"), quarterly_metrics.index))
    quarterly_metrics.columns = list(map(lambda x: ''.join(e for e in x.lower().replace(" ", "_") if e.isalnum() or e == "_"), quarterly_metrics.columns))
    quarterly_metrics = quarterly_metrics.rename_axis("month").reset_index()
    return quarterly_metrics

def getTickers(bucket_name: str, key: str, s3=None):
    '''
    Input: The bucket and key for the list of tickers
    Output: A list of tickers
    '''
    if s3 is None:
        s3 = boto3.client('s3')
    
    try:
        obj = s3.get_object(Bucket=args.bucket_name, Key=args.ticker_list_location)
        tickers = list(map(lambda x: str(x).upper(), pd.read_csv(obj['Body']).ticker_name.values))
        return tickers
    except Exception as e:
        print("Failed to obtain ticker list")
        return []

def uploadFiles(source_path: str, bucket_name: str, key: str, dest_path: str, s3=None):
    '''
    Input: Path on Local with all the files, the S3 bucket where it must be uploaded and the key for the files
    Output: No Output
    '''
    if s3 is None:
        s3 = boto3.client('s3')

    try:
        # If the directory is already existing, empty it first
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=key)
        if response["KeyCount"]:
            for object in response['Contents']:
                s3.delete_object(Bucket=bucket_name, Key=object['Key'])

        # Uploading all the files from the source path to specified key on S3
        for root, _, files in os.walk(source_path):
            for file in files:
                s3.upload_file(os.path.join(root, file), bucket_name, os.path.join(root, file).replace(dump_path, dest_path).strip("/"))
        return 1

    except Exception as e:
        print(e)
        return 0


# Consuming user-defined arguments
# Example: python extract.py --num_batches 10 "stock-price-dashboard" "ticker_list/ticker_list.csv" "raw_datasets"
parser = argparse.ArgumentParser()
parser.add_argument("--num_batches", type=int, default=10)      # Setting batch size for quicker processing
parser.add_argument("bucket_name")                              # Source & destination bucket name
parser.add_argument("ticker_list_location")                     # Key for the ticker list
parser.add_argument("raw_file_destination")                     # Key for the destination 
args = parser.parse_args()

# Obtaining the list of tickers
tickers = getTickers(args.bucket_name, args.ticker_list_location)
print("Obtained the list of Tickers")

# Creating empty directories where these files will be stored
dump_path = os.path.join(os.getcwd(), "raw_datasets")
if os.path.isdir(dump_path):
    shutil.rmtree(dump_path)

os.mkdir(dump_path)
os.mkdir(os.path.join(dump_path, "info"))
os.mkdir(os.path.join(dump_path, "stock_price"))
os.mkdir(os.path.join(dump_path, "financial_statements"))

# Implementing batching to reduce the RAM utilization
batches = int(args.num_batches)
batches = list(map(lambda x: int(x), np.linspace(0, len(tickers) + 1, batches + 1)))
batches = zip(batches[:-1], batches[1:])
for index, (i, j) in enumerate(batches):
    # For each batch, we will execute the three functions
    print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} : Started Batch {index + 1} - [{i}, {j-1}]")
    try:
        print(f"Extracting Instantaneous Information")
        getDescription(tickers[i:j]).to_parquet(os.path.join(os.path.join(dump_path, "info"), f"part-{index}.parquet"), index=False)
        print(f"Extracting Monthly Stock Summary")
        getStock(tickers[i:j], 2, "1mo").to_parquet(os.path.join(os.path.join(dump_path, "stock_price"), f"part-{index}.parquet"), index=False)
        print(f"Extracting Quarterly Financial Document Summary")
        getQuarterlyInformation(tickers[i:j]).to_parquet(os.path.join(os.path.join(dump_path, "financial_statements"), f"part-{index}.parquet"), index=False)
        print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} : Completed Batch {index + 1}\n")
    except Exception as e:
        # In case of any failures, we will now to the next batch
        print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} : Failed Batch {index + 1}\n")
        print(e)

if uploadFiles(dump_path, args.bucket_name, args.raw_file_destination, args.raw_file_destination):
    print("Successfully uploaded files to S3")
    print("Step 1 of ETL Completed")
    with open("extract_step_completed.txt", "w+") as lambda_file:
        lambda_file.write(f"Completed on: {datetime.datetime.now()}")

    s3 = boto3.client('s3')
    s3.upload_file("extract_step_completed.txt", args.bucket_name, f"{args.raw_file_destination}/extract_step_completed.txt")
else:
    print("S3 Upload has failed")
