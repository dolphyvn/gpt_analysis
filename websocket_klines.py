import os
import pathlib
import asyncio
from typing import List
from binance import AsyncClient, BinanceSocketManager
from dotenv import load_dotenv
from loguru import logger
from collections import defaultdict
import pandas as pd

from datetime import datetime


import pandas as pd

def calculate_volume_profile(klines_df):
    # Convert the epoch timestamp to human-readable datetime format
    klines_df['Open_Time'] = pd.to_datetime(klines_df['Open_Time'], unit='ms')
    
    # Original calculations
    # 1. Calculate total volume by price
    vp = klines_df.groupby('Close').agg({'Volume': 'sum'}).sort_index()

    # 2. Determine the Point of Control (POC)
    poc_price = vp['Volume'].idxmax()

    # 3. & 4. Calculate the Value Area
    vp['Cumulative_Volume'] = vp['Volume'].cumsum()
    total_volume = vp['Cumulative_Volume'].max()
    vp['Distance from POC'] = abs(vp['Cumulative_Volume'] - total_volume * 0.70)

    # Assuming the data is dense enough, the 70% volume around the POC can be found as:
    upper_value_area = vp[vp['Cumulative_Volume'] > total_volume * 0.35].index.min()
    lower_value_area = vp[vp['Cumulative_Volume'] < total_volume * 0.35].index.max()

    # 5. & 6. Identify the High Profile (HP) and Low Profile (LP)
    high_profile = klines_df['High'].max()
    low_profile = klines_df['Low'].min()

    # Create a new DataFrame for timestamps and volumes for each interval
    comprehensive_df = klines_df[['Open_Time', 'Close', 'Volume']]
    
    # Merge it with the volume profile DataFrame
    comprehensive_df = pd.merge(comprehensive_df, vp, left_on='Close', right_index=True, how='left')
    
    # Broadcast the metric values across all rows of the dataframe
    comprehensive_df['POC'] = poc_price
    comprehensive_df['VAH'] = upper_value_area
    comprehensive_df['VAL'] = lower_value_area
    comprehensive_df['HP'] = high_profile
    comprehensive_df['LP'] = low_profile

    return comprehensive_df



async def fetch_historical_data(symbol: str, interval: str, limit: int = 1000):
    client = await AsyncClient.create()
    klines = await client.futures_klines(symbol=symbol, interval=interval, limit=limit)
    await client.close_connection()
    return klines


async def main(symbols: List[str]):
    print(f'Started Collecting Tick Data of {symbols}...')

    # Fetch historical data (30 days of daily data)
    for symbol in symbols:
        print(f"Fetching historical data for {symbol}")
        klines = await fetch_historical_data(symbol, interval='1d', limit=30)
        # Create a DataFrame
        columns = ["Open_Time", "Open", "High", "Low", "Close", "Volume", "Close_Time", "Quote_Asset_Volume", "Number_of_Trades", "Taker_Buy_Base_Asset_Volume", "Taker_Buy_Quote_Asset_Volume", "Ignore"]
        df = pd.DataFrame(klines, columns=columns)
        df[['Open', 'High', 'Low', 'Close', 'Volume']] = df[['Open', 'High', 'Low', 'Close', 'Volume']].apply(pd.to_numeric)

        # print(klines)
        volume_profile = calculate_volume_profile(df)
        # Renaming columns for clarity
        volume_profile.rename(columns={
            'Volume_x': 'Daily_Volume',
            'Volume_y': 'Aggregated_Volume_by_Close_Price'
        }, inplace=True)
        print(f"Volume profile for {symbol}:", volume_profile)  # Print top 10 volume profiles

if __name__ == "__main__":
    load_dotenv()
    logger.remove()

    symbol_list = [s.lower().strip() for s in os.getenv("symbols").split(",")]


    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(symbol_list))
