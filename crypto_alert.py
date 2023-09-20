import os
import pathlib
import asyncio
from typing import List
from binance import AsyncClient, BinanceSocketManager
from dotenv import load_dotenv
from collections import defaultdict
import pandas as pd
from utils import store_klines_to_db, vsa_volume, check_spikes,send_to_telegram
from datetime import datetime
from mysql_connector import store_klines_to_mysql
# from vp import calculate_vp
import pandas as pd
from utils import calculate_advanced_volume_profile as cavp
# from open_ai import *
import time
import requests



def get_limit_from_interval(interval: str) -> int:
    """
    Get the corresponding data limit based on the interval for 30 days of data.

    Args:
    - interval (str): The interval (e.g., "1h", "5m", "4h", "1w", "1M").

    Returns:
    - int: The data limit corresponding to 30 days of data for the given interval.
    """
    
    conversions = {
        '1m': 120,
        '5m': 60,
        '15m': 120,
        '30m': 60,
        '1h': 720,
        '3h': 240,
        '4h': 180,
        '6h': 120,
        '8h': 90,
        '12h': 60,
        '1d': 240,
        '1w': 240,
        '1M': 120
    }
    
    return conversions.get(interval, 720)  # default to 720 if interval is not found

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
            time.sleep(5)
            print(f"Fetching historical data for {symbol} with interval {interval} and limit {limit}")
            klines = await fetch_historical_data(symbol, interval=interval, desired_limit=limit)

            # Create a DataFrame
            columns = ["Open_Time", "Open", "High", "Low", "Close", "Volume", "Close_Time", "Quote_Asset_Volume", "Number_of_Trades", "Taker_Buy_Base_Asset_Volume", "Taker_Buy_Quote_Asset_Volume", "Ignore"]
            df = pd.DataFrame(klines, columns=columns)
            df[['Open', 'High', 'Low', 'Close', 'Volume']] = df[['Open', 'High', 'Low', 'Close', 'Volume']].apply(pd.to_numeric)
            if os.getenv('storage') == 'sqlite3':
                store_klines_to_db(klines,interval,symbol)
            if os.getenv('storage') == 'mysql':
                store_klines_to_mysql(klines,interval,symbol)
            else:
                # df = vsa_volume(df)
                last_rows = check_spikes(df,2)
                for _, row in last_rows.iterrows():
                    if row['Result_Bearish'] or row['Result_Bullish']:
                        send_to_telegram(symbol)

if __name__ == "__main__":
    load_dotenv()
    # logger.remove()

    # Load symbol from .env
    symbol_list = [s.upper().strip() for s in os.getenv("symbols").split(",")]

    # Load intervals from .env
    intervals_list = [i.strip() for i in os.getenv("intervals").split(",")]

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(symbol_list,intervals_list))
