import requests
import os
import zipfile
from datetime import datetime, timedelta
from dotenv import load_dotenv
from utils import store_aggregated_trades_to_db
from mysql_connector import store_aggregated_trades_to_mysql

BASE_URL = "https://data.binance.vision/data/futures/um/"

# DATA_TYPES = ["aggTrades", "bookTicker", "metrics", "liquidationSnapshot"]

DATA_TYPES = ["aggTrades"]

import csv

def combine_csv_files(file_paths, output_file_name):
    combined_data = []
    for file_path in file_paths:
        with open(file_path, 'r') as f:
            reader = csv.reader(f)
            data = list(reader)
            if combined_data:  # If combined_data is not empty
                data = data[1:]  # remove the header
            combined_data.extend(data)
    with open(output_file_name, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(combined_data)


def read_csv_and_store(file_name, symbol):
    with open(file_name, 'r') as csv_file:
        # Use csv.reader to read the file
        reader = csv.reader(csv_file)
        
        # Skip the header
        next(reader)
        
        # Process the CSV rows
        aggregated_trades_data = []
        for row in reader:
            # Convert the "is_buyer_maker" column from string 'true' or 'false' to int (1 or 0)
            is_buyer_maker = 1 if row[6].lower() == 'true' else 0
            trade = (symbol,) + tuple(row[:6]) + (is_buyer_maker,)
            aggregated_trades_data.append(trade)
        
        # Now, store this processed data in the DB
        if os.getenv('storage') == 'mysql':
            store_aggregated_trades_to_mysql(aggregated_trades_data, symbol)
        if os.getenv('storage') == 'sqlite3':
            store_aggregated_trades_to_db(aggregated_trades_data, symbol)        

def download_data(data_type, symbol, start_date, end_date,freq):
    """
    Download data from Binance for a given symbol, type, and date range.
    """
    current_date = start_date
    symbol_csv_files = []

    while current_date <= end_date:
        # Determine if the data is daily or monthly
        # if current_date.day == 1 and current_date + timedelta(days=28) <= end_date:  # Approximately a month
        if freq == 'monthly':
            freq = "monthly"
            filename = f"{symbol}-{data_type}-{current_date.year}-{str(current_date.month).zfill(2)}.zip"
            current_date += timedelta(days=30)  # Increment by roughly a month
        else:
            freq = "daily"
            filename = f"{symbol}-{data_type}-{current_date.year}-{str(current_date.month).zfill(2)}-{str(current_date.day).zfill(2)}.zip"
            current_date += timedelta(days=1)  # Increment by a day
        
        # Build URL
        url = os.path.join(BASE_URL, freq, data_type, symbol, filename)
        
        # Download the zip file
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Failed to download data for {current_date.strftime('%Y-%m-%d')}. Skipping...")
            continue
        
        with open(filename, 'wb') as file:
            file.write(response.content)
        
        # Unzip the file
        with zipfile.ZipFile(filename, 'r') as zip_ref:
            # Define extract path
            # extract_path = os.path.join("data","futures", symbol.replace("USDT", "_USDT"))
            extract_path = os.path.join("data","futures")
            # Ensure the path exists
            if not os.path.exists(extract_path):
                os.makedirs(extract_path)

            zip_ref.extractall(extract_path)

            # Since a zip might have multiple files, get the list of files
            csv_files = [name for name in zip_ref.namelist() if name.endswith('.csv')]
            
            # Store the contents of each csv in the database
            for csv_file in csv_files:
                csv_path = os.path.join(extract_path, csv_file)
                symbol_csv_files.append(csv_path)
                if os.getenv('storage') == 'csv':
                    print('Data saved')
                else:
                    read_csv_and_store(csv_path, symbol)
                    os.remove(csv_path)  # If you wish to remove the csv file after processing
        
        os.remove(filename)
        print(f"Successfully downloaded and extracted data for {current_date.strftime('%Y-%m-%d')}.")
        

    print(f"List of all csv files {symbol_csv_files} of {symbol}")

    if os.getenv('storage') == 'csv':
        # ... at the end of your for-loop iterating over the symbols:
        combined_file_name = os.path.join("data", "futures", f"{symbol}_{os.getenv('frequency')}_combined.csv")
        combine_csv_files(symbol_csv_files, combined_file_name)
        print(f"All data for {symbol} combined and saved to {combined_file_name}.")
        # ... after combining the files:
        for file_path in symbol_csv_files:
            os.remove(file_path)
        # Reset the list for the next symbol
        symbol_csv_files = []



load_dotenv()
# Usage example
symbol_list = [s.strip() for s in os.getenv("symbols").split(",")]
lookback = int(os.getenv('lookback'))
freq = os.getenv('frequency')
for symbol in symbol_list:
    # Set end_date to today's date
    end_date = datetime.today().date()
    
    # Set start_date to 29 days before the end_date (to make a range of 30 days including the end date)
    start_date = end_date - timedelta(days=int(os.getenv('lookback')))

    for data_type in DATA_TYPES:
        download_data(data_type, symbol, start_date, end_date,freq=os.getenv('frequency'))
