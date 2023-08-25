import json
import os
import sqlite3
from dotenv import load_dotenv
from polygon import RESTClient
import pandas as pd  # Import pandas library
from utils import calculate_volume_profile_fromklines,store_to_file,print_in_chunks
from datetime import datetime, timedelta


def create_db_connection(db_name):
    return sqlite3.connect(db_name)

def create_table(conn):
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS aggs (
        ticker TEXT,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume REAL,
        vwap REAL,
        timestamp INTEGER PRIMARY KEY,
        transactions INTEGER,
        otc TEXT
    )''')
    conn.commit()

def insert_data(conn, data):
    cursor = conn.cursor()
    cursor.executemany('''INSERT OR IGNORE INTO aggs 
        (ticker, open, high, low, close, volume, vwap, timestamp, transactions, otc)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', data)
    conn.commit()

def get_data(ticker, interval, lookback):
    load_dotenv()
    api_key = os.getenv("POLYGON_API_KEY")
    client = RESTClient(api_key=api_key)

    end_date = datetime.today().date()
    start_date = end_date - timedelta(days=int(lookback))

    aggs_data = []
    for a in client.list_aggs(ticker=ticker, multiplier=1, timespan=interval, from_=start_date, to=end_date, limit=50000):
        aggs_data.append((
            ticker, a.open, a.high, a.low, a.close, a.volume, a.vwap, a.timestamp, a.transactions, a.otc
        ))

    df = pd.DataFrame(aggs_data, columns=['Ticker', 'Open', 'High', 'Low', 'Close', 'Volume', 'VWAP', 'Timestamp', 'Transactions', 'OTC'])
    df['Open_Time'] = df['Timestamp']

    volume_profile = calculate_volume_profile_fromklines(df)
    volume_profile.rename(columns={
        'Volume_x': 'Volume',
        'Volume_y': 'Aggregated_Volume_by_Close_Price'
    }, inplace=True)

    # Convert the DataFrame to a JSON-formatted string
    data = volume_profile.to_csv(index=False)
    
    return data
    # if os.getenv('storage') == 'sqlite3':
    #     # Setup SQLite database
    #     conn = create_db_connection('aggs_data.db')
    #     create_table(conn)
    #     insert_data(conn, aggs_data)
    #     conn.close()
    # else:
    #     store_to_file(volume_profile,ticker,interval)
    #     # print(f"Volume profile for {symbol}:", volume_profile)  # Print top 10 volume profiles
    #     print_in_chunks(volume_profile)


# if __name__ == "__main__":
#     load_dotenv()
#     api_key = os.getenv("POLYGON_API_KEY")
#     client = RESTClient(api_key=api_key)
#     ticker = "C:XAUUSD"
#     interval = 'day'
#     lookback = 120
#     get_data(ticker,interval,lookback)

    # # List Aggregates (Bars)
    # aggs_data = []
    # for a in client.list_aggs(ticker=ticker, multiplier=1, timespan=interval, from_="2023-03-01", to="2023-08-25", limit=50000):
    #     aggs_data.append((
    #         ticker, a.open, a.high, a.low, a.close, a.volume, a.vwap, a.timestamp, a.transactions, a.otc
    #     ))

    # # Create a DataFrame from the aggs_data list
    # df = pd.DataFrame(aggs_data, columns=['Ticker', 'Open', 'High', 'Low', 'Close', 'Volume', 'VWAP', 'Timestamp', 'Transactions', 'OTC'])
    # df['Open_Time'] = df['Timestamp']
    # print(df[['Open_Time','Open', 'High', 'Low', 'Close', 'Volume']])
    # volume_profile = calculate_volume_profile_fromklines(df)
    # # Renaming columns for clarity
    # volume_profile.rename(columns={
    #     'Volume_x': 'Volume',
    #     'Volume_y': 'Aggregated_Volume_by_Close_Price'
    # }, inplace=True)
    # if os.getenv('storage') == 'sqlite3':
    #     # Setup SQLite database
    #     conn = create_db_connection('aggs_data.db')
    #     create_table(conn)
    #     insert_data(conn, aggs_data)
    #     conn.close()
    # else:
    #     store_to_file(volume_profile,ticker,interval)
    #     # print(f"Volume profile for {symbol}:", volume_profile)  # Print top 10 volume profiles
    #     print_in_chunks(volume_profile)


