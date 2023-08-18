import requests
import os
import zipfile
from datetime import datetime, timedelta

# https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md

BASE_URL = "https://data.binance.vision/data/futures/um/"

DATA_TYPES = ["aggTrades", "bookTicker", "metrics", "liquidationSnapshot"]

def download_data(data_type, symbol, start_date, end_date):
    """
    Download data from Binance for a given symbol, type, and date range.
    """
    current_date = start_date
    
    while current_date <= end_date:
        # Determine if the data is daily or monthly
        if current_date.day == 1 and current_date + timedelta(days=28) <= end_date:  # Approximately a month
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
            extract_path = os.path.join("data","futures", symbol.replace("USDT", "_USDT"))
            # Ensure the path exists
            if not os.path.exists(extract_path):
                os.makedirs(extract_path)
            
            zip_ref.extractall(extract_path)  # Extract to the specified directory
        
        os.remove(filename)  # Remove the zip file
        print(f"Successfully downloaded and extracted data for {current_date.strftime('%Y-%m-%d')}.")

# Usage example
symbol = "BTCUSDT"
start_date = datetime(2023, 8, 15)
end_date = datetime(2023, 8, 17)

for data_type in DATA_TYPES:
    download_data(data_type, symbol, start_date, end_date)
