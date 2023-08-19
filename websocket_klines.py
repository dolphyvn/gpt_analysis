import os
import pathlib
import asyncio
from typing import List
from binance import AsyncClient, BinanceSocketManager
from dotenv import load_dotenv
from loguru import logger
from collections import defaultdict
import pandas as pd
from utils import store_klines_to_db
from datetime import datetime


import pandas as pd
def calculate_volume_profile(df):
    # Convert the epoch timestamp to human-readable datetime format
    POCs, VAHs, VALs, HVNs, LVNs, Volumes = [], [], [], [], [], []
    Opens, Highs, Lows, Closes = [], [], [], []
    df['Open_Time'] = pd.to_datetime(df['Open_Time'], unit='ms')
    
    # Loop through the DataFrame
    for idx, row in df.iterrows():
        # Get data only for the current month (row)
        current_data = df.loc[idx:idx] 
        volume_price = current_data.groupby('Close')['Volume'].sum().sort_values(ascending=False)
        total_volume = volume_price.sum()
        
        # Point of Control (POC)
        POC_price = volume_price.idxmax() if not volume_price.empty else row['Close'] 
        
        # Calculate VAH and VAL
        sorted_volume = volume_price.sort_index(ascending=False)
        sorted_volume_cumsum = sorted_volume.cumsum()
        value_area = sorted_volume[sorted_volume_cumsum <= total_volume*0.7]
        VAH_price = value_area.index.max() if not value_area.empty else row['High']
        VAL_price = value_area.index.min() if not value_area.empty else row['Low']

        # Calculating HVN and LVN for the current interval
        volume_diff = sorted_volume.diff().fillna(0)
        HVN_price = volume_diff.idxmax() if not volume_diff.empty else row['High']
        LVN_price = volume_diff.idxmin() if not volume_diff.empty else row['Low']

        # Append the results and OHLC to the lists
        POCs.append(POC_price)
        VAHs.append(VAH_price)
        VALs.append(VAL_price)
        HVNs.append(HVN_price)
        LVNs.append(LVN_price)
        Volumes.append(current_data['Volume'].sum())  # Adding cumulative volume for each day

        Opens.append(row['Open'])
        Highs.append(row['High'])
        Lows.append(row['Low'])
        Closes.append(row['Close'])

    # Create a new DataFrame with the results
    result_df = pd.DataFrame({
        'Open_Time': df['Open_Time'],
        'Open': Opens,
        'High': Highs,
        'Low': Lows,
        'Close': Closes,
        'POC': POCs,
        'VAH': VAHs,
        'VAL': VALs,
        'HVN': HVNs,
        'LVN': LVNs,
        'Volume': Volumes
    })

    return result_df

def store_to_file(volume_profile: pd.DataFrame, symbol: str,interval: str, output_dir: str = './output'):
    """
    Store the volume profile DataFrame to a CSV file.

    Args:
    - volume_profile (pd.DataFrame): The DataFrame containing the volume profile.
    - symbol (str): The symbol (e.g., "BTCUSDT") used for naming the file.
    - output_dir (str): Directory where the CSV files will be stored. Default is './output'.

    Returns:
    - None
    """
    
    # Check if output directory exists, if not, create it.
    pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Define file path
    file_path = os.path.join(output_dir, f"{symbol}_{interval}_volume_profile.csv")

    # Store DataFrame to CSV
    volume_profile.to_csv(file_path, index=False)
    print(f"Stored volume profile for {symbol} in {file_path}")


def get_limit_from_interval(interval: str) -> int:
    """
    Get the corresponding data limit based on the interval for 30 days of data.

    Args:
    - interval (str): The interval (e.g., "1h", "5m", "4h", "1w", "1M").

    Returns:
    - int: The data limit corresponding to 30 days of data for the given interval.
    """
    
    conversions = {
        '1m': 43200,
        '5m': 8640,
        '15m': 2880,
        '30m': 1440,
        '1h': 720,
        '3h': 240,
        '4h': 180,
        '6h': 120,
        '8h': 90,
        '12h': 60,
        '1d': 30,
        '1w': 4,
        '1M': 1
    }
    
    return conversions.get(interval, 720)  # default to 720 if interval is not found



def print_in_chunks(df, chunk_size=50):
    if len(df) <= chunk_size:
        print(df)
        return

    num_chunks = (len(df) - 1) // chunk_size + 1
    
    for i in range(num_chunks):
        start_idx = i * chunk_size
        end_idx = start_idx + chunk_size
        print(f"Part {i}:")
        print(df[start_idx:end_idx])

# async def fetch_historical_data(symbol: str, interval: str, limit: int = 1000):
#     client = await AsyncClient.create()
#     klines = await client.futures_klines(symbol=symbol, interval=interval, limit=limit)
#     await client.close_connection()
#     return klines

async def fetch_historical_data(symbol: str, interval: str, desired_limit: int = 1000):
    client = await AsyncClient.create()
    max_api_limit = 1000  # This is the typical limit for many endpoints
    klines = []

    if desired_limit <= max_api_limit:
        klines = await client.futures_klines(symbol=symbol, interval=interval, limit=desired_limit)
    else:
        loops_required = int(desired_limit / max_api_limit)
        remaining_data = desired_limit

        for _ in range(loops_required):
            current_limit = min(max_api_limit, remaining_data)  # Fetch only the remaining data if less than max_api_limit
            current_klines = await client.futures_klines(symbol=symbol, interval=interval, limit=current_limit)
            klines.extend(current_klines)
            
            if len(current_klines) < current_limit:  # This means we've fetched all available data
                break
            
            remaining_data -= len(current_klines)
            await asyncio.sleep(1)  # Add a delay to avoid hitting rate limits
    
    await client.close_connection()
    return klines



async def main(symbols: List[str],intervals_list: List[str]):
    print(f'Started Collecting Tick Data of {symbols}...')
    for interval in intervals_list:
        limit = get_limit_from_interval(interval)

        # Fetch historical data (M=month,w=week,d=day,h=hour,m=minute)
        for symbol in symbols:
            print(f"Fetching historical data for {symbol} with interval {interval} and limit {limit}")
            klines = await fetch_historical_data(symbol, interval=interval, desired_limit=limit)

            # Create a DataFrame
            columns = ["Open_Time", "Open", "High", "Low", "Close", "Volume", "Close_Time", "Quote_Asset_Volume", "Number_of_Trades", "Taker_Buy_Base_Asset_Volume", "Taker_Buy_Quote_Asset_Volume", "Ignore"]
            df = pd.DataFrame(klines, columns=columns)
            df[['Open', 'High', 'Low', 'Close', 'Volume']] = df[['Open', 'High', 'Low', 'Close', 'Volume']].apply(pd.to_numeric)

            # print(klines)
            store_klines_to_db(klines,symbol,interval)
            # volume_profile = calculate_volume_profile(df)

            # # Renaming columns for clarity
            # volume_profile.rename(columns={
            #     'Volume_x': 'Volume',
            #     'Volume_y': 'Aggregated_Volume_by_Close_Price'
            # }, inplace=True)

            # store_to_file(volume_profile,symbol,interval)
            # # print(f"Volume profile for {symbol}:", volume_profile)  # Print top 10 volume profiles
            # print_in_chunks(volume_profile)

if __name__ == "__main__":
    load_dotenv()
    logger.remove()

    # Load symbol from .env
    symbol_list = [s.lower().strip() for s in os.getenv("symbols").split(",")]

    # Load intervals from .env
    intervals_list = [i.strip() for i in os.getenv("intervals").split(",")]

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(symbol_list,intervals_list))
