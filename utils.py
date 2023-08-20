import pandas as pd
from collections import defaultdict
from datetime import datetime
import os

# aggregated data
# Field Name  Description
# agg_trade_id    Aggregated Trade ID
# price   Price
# quantity    Quantity
# first_trade_id  First Trade ID
# last_trade_id   Last Trade ID
# transact_time   Transaction time in unix format
# is_buyer_maker  Was the buyer the maker

# https://data.binance.vision/data/futures/um/daily/bookTicker/BTCUSDT/BTCUSDT-bookTicker-2023-08-16.zip
# BookTicker data
# Field Name  Description
# update_id   Orderbook Update ID
# best_bid_price  Best Bid Price
# best_bid_qty    Best Bid Quantity
# best_ask_price  Best Ask Price
# best_ask_qty    Best Ask Quantity
# transaction_time    Transaction Time
# event_time  Event Time


# https://data.binance.vision/data/futures/um/daily/metrics/BTCUSDT/BTCUSDT-metrics-2023-08-16.zip
# Trading metrics
# Field Name  Description
# create_time Create Time
# symbol  Symbol
# sum_open_interest   Total Open Interest (Base)
# sum_open_interest_value Total Open Interest Value
# count_toptrader_long_short_ratio    Long/Short account num ratio of top traders
# sum_toptrader_long_short_ratio  Long/Short position ratio of top traders
# count_long_short_ratio  Long/Short account num ratio of all traders
# sum_taker_long_short_vol_ratio  Taker total buy volume/ taker total sell volume

# https://data.binance.vision/data/futures/um/daily/liquidationSnapshot/BTCUSDT/BTCUSDT-liquidationSnapshot-2023-08-16.zip
# Liquidation Snapshot 
# Field Name  Description
# time    Order Time
# symbol  Symbol
# side    Order Side
# order_type  Order Type
# time_in_force   Time In Force
# original_quantity   Original Order Quantity
# price   Order Price
# average_price   Average Fill Price
# order_status    Order Status
# last_fill_quantity  Last Trade Order Quantity
# accumulated_fill_quantity   Order Accumulated Fill Quantity


# import pandas as pd
import datetime

import csv



def volume_by_price(df, interval='30T'):
    # Assuming df has 'price', 'quantity' columns and a datetime index
    
    # Group by the specified time interval and aggregate price and volume information
    grouped = df.groupby(pd.Grouper(freq=interval)).agg({'price': ['min', 'max'], 'quantity': 'sum'}).dropna()
    
    # Initialize the volume profile dictionary
    volume_profile = {}

    for _, row in grouped.iterrows():
        min_price, max_price, volume = row[('price', 'min')], row[('price', 'max')], row[('quantity', 'sum')]
        price_range = pd.interval_range(start=min_price, end=max_price, freq='1T')  # 1T means 1 unit of price, adjust as needed

        for price_interval in price_range:
            center_price = (price_interval.left + price_interval.right) / 2
            volume_profile[center_price] = volume_profile.get(center_price, 0) + volume / len(price_range)
    
    # Convert dictionary to DataFrame
    vp_df = pd.DataFrame(list(volume_profile.items()), columns=['Price', 'Volume']).sort_values(by='Price')
    
    # Calculate Point of Control (POC)
    poc_price = vp_df.loc[vp_df['Volume'].idxmax()]['Price']
    
    # Calculate Value Area
    vp_df_sorted = vp_df.sort_values(by='Volume', ascending=False)
    vp_df_sorted['Cumulative Volume'] = vp_df_sorted['Volume'].cumsum()
    value_area_df = vp_df_sorted[vp_df_sorted['Cumulative Volume'] <= vp_df_sorted['Cumulative Volume'].iloc[-1] * 0.7]
    value_area_high = value_area_df['Price'].max()
    value_area_low = value_area_df['Price'].min()
    
    # Identify HVN and LVN
    vp_df['Volume Diff'] = vp_df['Volume'].diff().fillna(0)
    hvn = vp_df[vp_df['Volume Diff'] > 0]['Price'].tolist()
    lvn = vp_df[vp_df['Volume Diff'] < 0]['Price'].tolist()

    results = {
        'POC': poc_price,
        'Value Area High': value_area_high,
        'Value Area Low': value_area_low,
        'High Volume Nodes': hvn,
        'Low Volume Nodes': lvn
    }

    # Convert results to DataFrame
    results_df = pd.DataFrame(list(results.items()), columns=['Metric', 'Value'])

    return results_df


def calculate_advanced_volume_profile(df, interval="30T"):
    # Calculate the total volume for the entire DataFrame
    total_volume = df['quantity'].sum()

    # Up/Down volume based on close and open comparison
    df['direction'] = ['Up' if close >= open else 'Down' for close, open in zip(df['price'], df['price'].shift(1))]
    df['up_volume'] = df['quantity'].where(df['direction'] == 'Up', 0)
    df['down_volume'] = df['quantity'].where(df['direction'] == 'Down', 0)

    # Resample the dataframe based on the provided interval
    df_resampled = df.resample(interval, on='transact_time').agg({
        'price': ['min', 'max'],
        'up_volume': 'sum',
        'down_volume': 'sum'
    })

    results = []

    for idx, row in df_resampled.iterrows():
        start_time = idx
        numeric_interval = int(interval[:-1])  # Strip out the last character and convert to integer
        end_time = start_time + pd.Timedelta(minutes=numeric_interval)
        
        interval_df = df[(df['transact_time'] >= start_time) & (df['transact_time'] < end_time)].copy()

        # Total volume within this interval
        interval_total_volume = interval_df['quantity'].sum()

        # Get Profile High and Low
        profile_high = row[('price', 'max')]
        profile_low = row[('price', 'min')]

        # Get POC: price level with the maximum traded volume
        poc = interval_df.groupby('price')['quantity'].sum().idxmax()

        # Calculate Value Area (70% of total volume)
        interval_df_grouped = interval_df.groupby('price').agg({'quantity': 'sum'}).sort_values(by='quantity', ascending=False)
        interval_df_grouped['cumulative_volume'] = interval_df_grouped['quantity'].cumsum()
        value_area_df = interval_df_grouped[interval_df_grouped['cumulative_volume'] <= interval_total_volume * 0.7]
        value_area_high = value_area_df.index.max()
        value_area_low = value_area_df.index.min()

        results.append({
            'Start_Time': start_time,
            'End_Time': end_time,
            'Profile_High': profile_high,
            'Profile_Low': profile_low,
            'POC': poc,
            'Value_Area_High': value_area_high,
            'Value_Area_Low': value_area_low,
            'Interval_Total_Volume': interval_total_volume
        })

    # Convert the results list of dictionaries to a DataFrame
    results_df = pd.DataFrame(results)
    
    return results_df


def calculate_tpo_agg(df):
    # Convert transact_time into a readable datetime format
    df['transact_time'] = pd.to_datetime(df['transact_time'], unit='ms')

    # Set time interval for TPOs
    interval = "30T"

    # Group by time interval and get the min, max prices for each interval
    price_ranges = df.groupby(pd.Grouper(key='transact_time', freq=interval)).agg({'price': ['min', 'max']})

    tpo_chart = {}
    letters = list('ABCDEFGHIJKLMNOPQRSTUVWXYZ')

    for i, (index, row) in enumerate(price_ranges.iterrows()):
        min_price, max_price = row['price']
        tpo_letter = letters[i % len(letters)]  # cycle through the letters
        
        for price in range(int(min_price), int(max_price) + 1):
            if price not in tpo_chart:
                tpo_chart[price] = []
            tpo_chart[price].append(tpo_letter)

    # Calculate Point of Control
    poc_price = max(tpo_chart, key=lambda k: len(tpo_chart[k]))
    
    # Calculate Value Area (let's assume top 70% for now, but this can be changed)
    all_tpos = sum([len(v) for v in tpo_chart.values()])
    tpo_sorted_by_count = sorted(tpo_chart.items(), key=lambda x: len(x[1]), reverse=True)
    current_tpo_count = 0
    value_area = []

    for price, tpos in tpo_sorted_by_count:
        current_tpo_count += len(tpos)
        value_area.append(price)
        if current_tpo_count >= all_tpos * 0.7:  # 70%
            break

    # Calculate Rotation Factor
    prev_count = len(tpo_chart[value_area[0]])
    rotation_factor = 0

    for price in value_area[1:]:
        curr_count = len(tpo_chart[price])
        rotation_factor += (curr_count - prev_count)
        prev_count = curr_count

    # Calculate Delta (difference between buying and selling TPOs)
    df['is_buyer'] = df['is_buyer_maker'].astype(int)
    df['is_seller'] = (~df['is_buyer_maker']).astype(int)
    total_buyers = df['is_buyer'].sum()
    total_sellers = df['is_seller'].sum()
    delta = total_buyers - total_sellers

    # Extract TPO letters and price range for each time interval
    tpo_letters_list = []
    price_range_list = []
    
    for index in price_ranges.index:
        min_price, max_price = price_ranges.loc[index, 'price']
        # tpo_letters = ''.join([tpo_chart.get(price, '') for price in range(int(min_price), int(max_price) + 1)])
        tpo_letters = ''.join([''.join(tpo_chart.get(price, '')) for price in range(int(min_price), int(max_price) + 1)])

        price_range = f"{min_price} - {max_price}"
        
        tpo_letters_list.append(tpo_letters)
        price_range_list.append(price_range)

    # Create and return a DataFrame with the results
    results_df = pd.DataFrame({
        'Time Interval': price_ranges.index,
        'TPO Letters': tpo_letters_list,
        'Price Range': price_range_list
    })


    summary_df = pd.DataFrame({
        'POC Price': [poc_price],
        'Value Area Min': [min(value_area)],
        'Value Area Max': [max(value_area)],
        'Rotation Factor': [rotation_factor],
        'Delta': [delta]
    })

    return results_df, summary_df



# csv_path = "path_to_your_csv.csv"
# tpo_chart, poc, value_area, rotation_factor, delta = calculate_tpo(csv_path)
# for price, tpos in sorted(tpo_chart.items(), reverse=True):
#     print(f"{price}: {' '.join(tpos)}")

# print("\nPoint of Control:", poc)
# print("Value Area:", value_area)
# print("Rotation Factor:", rotation_factor)
# print("Delta:", delta)


def read_data(filename):
    df = pd.read_csv(filename)
    df['transact_time'] = pd.to_datetime(df['transact_time'], unit='ms')
    return df

def read_bookticker_data(filename):
    df = pd.read_csv(filename)
    df['transaction_time'] = pd.to_datetime(df['transaction_time'], unit='ms')
    return df

# generate footprint candle from aggregated data
def footprint_candle_agg(df, time_interval):

    # Convert transaction time to datetime and create time interval
    df['transact_time'] = pd.to_datetime(df['transact_time'], unit='ms')
    df['time_interval'] = df['transact_time'].dt.floor(time_interval)

    # Calculate buy and sell pressure based on whether trade was made by buyer or seller
    df['buy_pressure'] = df.apply(lambda x: x['quantity'] if not x['is_buyer_maker'] else 0, axis=1)
    df['sell_pressure'] = df.apply(lambda x: x['quantity'] if x['is_buyer_maker'] else 0, axis=1)

    # Group by the time intervals and aggregate
    footprint_df = df.groupby('time_interval').agg({
        'buy_pressure': 'sum',
        'sell_pressure': 'sum',
        'price': ['first', 'last']
    }).reset_index()
    footprint_df.columns = ['time_interval', 'buy_pressure', 'sell_pressure', 'open', 'close']

    footprint_df['delta'] = footprint_df['buy_pressure'] - footprint_df['sell_pressure']
    footprint_df['volume'] = footprint_df['buy_pressure'] + footprint_df['sell_pressure']

    return footprint_df[['time_interval', 'open', 'close', 'buy_pressure', 'sell_pressure', 'delta', 'volume']]


# generate footprint candle from bookticker data
def calculate_footprint(df, time_interval):
    df['transaction_time'] = pd.to_datetime(df['transaction_time'], unit='ms')
    df['time_interval'] = df['transaction_time'].dt.floor(time_interval)
    
    # Calculate changes in bid and ask quantities
    df['bid_qty_change'] = df.groupby('time_interval')['best_bid_qty'].diff().fillna(0)
    df['ask_qty_change'] = df.groupby('time_interval')['best_ask_qty'].diff().fillna(0)
    
    # Use changes in bid and ask quantities as proxies for buy and sell pressures
    df['buy_pressure'] = df['bid_qty_change'].apply(lambda x: x if x > 0 else 0)
    df['sell_pressure'] = df['ask_qty_change'].apply(lambda x: -x if x < 0 else 0)

    # Group by the time intervals and aggregate the buy and sell pressures
    footprint_df = df.groupby('time_interval').agg({
        'buy_pressure': 'sum',
        'sell_pressure': 'sum',
        'best_bid_price': 'first',
        'best_ask_price': 'last'
    }).reset_index()

    footprint_df['delta'] = footprint_df['buy_pressure'] - footprint_df['sell_pressure']
    footprint_df['volume'] = footprint_df['buy_pressure'] + footprint_df['sell_pressure']

    return footprint_df[['time_interval', 'best_bid_price', 'best_ask_price', 'buy_pressure', 'sell_pressure', 'delta', 'volume']]



# Daily Footprint Candle
def calculate_daily_footprint(df, price_increment=0.5):
    # Create a price level based on increments of price_increment
    df['price_level'] = (df['price'] // price_increment) * price_increment

    df['date'] = pd.to_datetime(df['transact_time'], unit='ms').dt.date
    aggregated = df.groupby(['date', 'price_level']).agg({
        'price': ['min', 'max'],
        'quantity': 'sum',
        'is_buyer_maker': [lambda x: (x == True).sum(), lambda x: (x == False).sum()]
    }).reset_index()

    aggregated.columns = ['date', 'price_level', 'low_price', 'high_price', 'total_volume', 'buy_volume', 'sell_volume']

    # Calculate Delta for each row
    aggregated['delta'] = aggregated['buy_volume'] - aggregated['sell_volume']

    # HVN and LVN for each date
    for date in aggregated['date'].unique():
        date_subset = aggregated[aggregated['date'] == date].copy()
        date_subset.sort_values(by='total_volume', ascending=False, inplace=True)
        
        # Assuming HVN is the price level with the highest volume and LVN with the lowest.
        # You can further adjust the definition if needed.
        hvn = date_subset.iloc[0]['price_level']
        lvn = date_subset.iloc[-1]['price_level']
        
        aggregated.loc[aggregated['date'] == date, 'hvn'] = hvn
        aggregated.loc[aggregated['date'] == date, 'lvn'] = lvn

    # Determine market dominance based on Delta
    aggregated['market_dominance'] = aggregated['delta'].apply(lambda x: 'Buyers' if x > 0 else 'Sellers' if x < 0 else 'Neutral')

    return aggregated

def calculate_daily_footprint_(data, price_increment=0.5, num_levels=5):
    data['Date'] = data['transact_time'].apply(lambda x: datetime.utcfromtimestamp(x/1000).strftime('%Y-%m-%d'))
    grouped_data = data.groupby('Date')
    results = []

    for date, group in grouped_data:
        volume_at_price = defaultdict(float)
        buy_volume = 0
        sell_volume = 0
        high = group['price'].max()
        low = group['price'].min()

        for _, row in group.iterrows():
            level = round(row['price'] / price_increment) * price_increment
            volume_at_price[level] += row['quantity']

            # Increment buy or sell volume based on is_buyer_maker
            if row['is_buyer_maker']:
                buy_volume += row['quantity']
            else:
                sell_volume += row['quantity']

        delta = buy_volume - sell_volume

        # Sorting by volume to determine POC and HVN/LVN
        sorted_volume = sorted(volume_at_price.items(), key=lambda x: x[1], reverse=True)
        poc = sorted_volume[0]

        # Assuming HVN is the top 10% of the volume nodes and LVN is the bottom 10%
        top_10_percent_index = len(sorted_volume) // 10
        hvn = sorted_volume[:top_10_percent_index]
        lvn = sorted_volume[-top_10_percent_index:]

        market_dominance = "Buyers" if delta > 0 else "Sellers" if delta < 0 else "Neutral"

        print(f"\nDate: {date}")
        print(f"High: {high}")
        print(f"Low: {low}")
        print(f"POC: {poc[0]} with volume: {poc[1]}")
        print(f"Delta: {delta}")
        print(f"Market was dominated by: {market_dominance}")
        print(f"HVN: {hvn}")
        print(f"LVN: {lvn}")

        # Print the price levels around the POC
        poc_index = [item[0] for item in sorted_volume].index(poc[0])
        start_idx = max(0, poc_index - num_levels)
        end_idx = min(poc_index + num_levels + 1, len(sorted_volume))
        for price, volume in sorted_volume[start_idx:end_idx]:
            print(f"Price Level: {price}, Volume: {volume}")

        results.append({
            'Date': date,
            'High': high,
            'Low': low,
            'POC': poc,
            'Volume Profile': volume_at_price,
            'Delta': delta,
            'Market Dominance': market_dominance,
            'HVN': hvn,
            'LVN': lvn
        })
        
    return results

# TPO
def calculate_TPO(df):
    tpo_letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    df['TPO'] = (df['transact_time'].dt.hour * 2) + (df['transact_time'].dt.minute // 30)
    df['TPO'] = df['TPO'].astype(int).apply(lambda x: tpo_letters[x % len(tpo_letters)])
    
    tpo_df = df.groupby([df['transact_time'].dt.date, 'price', 'TPO']).size().unstack().fillna('')
    return tpo_df

# Volume Profile
def calculate_VP(df):
    df['date'] = df['transact_time'].dt.date
    vp_df = df.groupby(['date', 'price'])['quantity'].sum().reset_index(name='total_volume')
    
    # Calculate value area, POC, high and low volume nodes for each day
    result = []
    for date, group in vp_df.groupby('date'):
        sorted_group = group.sort_values('total_volume', ascending=False)
        poc = sorted_group.iloc[0]
        total_volume = sorted_group['total_volume'].sum()
        sorted_group['cumulative_volume'] = sorted_group['total_volume'].cumsum()
        value_area_df = sorted_group[sorted_group['cumulative_volume'] <= total_volume * 0.7]
        
        high_volume_node = value_area_df['price'].max()
        low_volume_node = value_area_df['price'].min()
        
        result.append({
            'date': date,
            'POC': poc['price'],
            'high_volume_node': high_volume_node,
            'low_volume_node': low_volume_node
        })
    
    value_area_df = pd.DataFrame(result)
    return value_area_df

def calculate_volume_profilev1(df):
    df['date'] = pd.to_datetime(df['transact_time'], unit='ms').dt.date
    volume_profile = df.groupby(['date', 'price'])['quantity'].sum().reset_index()

    # Calculate additional metrics
    for date in volume_profile['date'].unique():
        date_subset = volume_profile[volume_profile['date'] == date].copy()
        poc_price = date_subset[date_subset['quantity'] == date_subset['quantity'].max()]['price'].values[0]
        total_volume = date_subset['quantity'].sum()
        date_subset['cumulative_volume'] = date_subset['quantity'].cumsum()
        value_area = date_subset[(date_subset['cumulative_volume'] <= total_volume * 0.7)]
        hvn = value_area['price'].max()
        lvn = value_area['price'].min()

        volume_profile.loc[volume_profile['date'] == date, 'POC'] = poc_price
        volume_profile.loc[volume_profile['date'] == date, 'Value_Area_High'] = hvn
        volume_profile.loc[volume_profile['date'] == date, 'Value_Area_Low'] = lvn

    return volume_profile

def calculate_volume_profile(df):
    df['datetime'] = pd.to_datetime(df['transact_time'], unit='ms')
    df['date'] = df['datetime'].dt.date
    volume_profile = df.groupby(['date', 'price'])['quantity'].sum().reset_index()

    # Calculate POC, value area, HVN, and LVN for each day
    results = []
    for date in volume_profile['date'].unique():
        date_subset = volume_profile[volume_profile['date'] == date].copy().sort_values(by='quantity', ascending=False)
        
        # POC
        poc = date_subset.iloc[0]['price']
        
        # Value Area (top 70% by volume)
        total_volume = date_subset['quantity'].sum()
        top_70_volume = 0.7 * total_volume
        current_volume = 0
        value_prices = []
        for _, row in date_subset.iterrows():
            current_volume += row['quantity']
            value_prices.append(row['price'])
            if current_volume >= top_70_volume:
                break
        value_area_min = min(value_prices)
        value_area_max = max(value_prices)
        
        # HVN & LVN
        avg_vol = date_subset['quantity'].mean()
        hvn = date_subset[date_subset['quantity'] >= 1.5 * avg_vol]['price'].tolist()
        lvn = date_subset[date_subset['quantity'] <= 0.5 * avg_vol]['price'].tolist()

        # Volume Gaps
        all_prices = sorted(date_subset['price'].tolist())
        gaps = [(all_prices[i-1], all_prices[i]) for i in range(1, len(all_prices)) if all_prices[i] - all_prices[i-1] > 1]
        
        results.append({
            'date': date,
            'POC': poc,
            'Value Area Min': value_area_min,
            'Value Area Max': value_area_max,
            'HVN': hvn,
            'LVN': lvn,
            'Volume Gaps': gaps,
            'Volume per Level': date_subset[['price', 'quantity']].values.tolist()
        })
        
    return pd.DataFrame(results)

import sqlite3

def store_klines_to_db(klines_data, interval, symbol, dbname="klines_data.db"):
    # Establish a connection to the SQLite database
    conn = sqlite3.connect(dbname)
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS klines (
        id INTEGER PRIMARY KEY,
        symbol TEXT NOT NULL,
        interval TEXT NOT NULL,
        open_time INTEGER NOT NULL,
        open REAL NOT NULL,
        high REAL NOT NULL,
        low REAL NOT NULL,
        close REAL NOT NULL,
        volume REAL NOT NULL,
        close_time INTEGER NOT NULL,
        quote_asset_volume REAL NOT NULL,
        trades INTEGER NOT NULL,
        taker_buy_base_asset_volume REAL NOT NULL,
        taker_buy_quote_asset_volume REAL NOT NULL,
        ignore_column REAL NOT NULL
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS aggregated_trades (
        agg_trade_id INTEGER PRIMARY KEY,
        symbol TEXT NOT NULL,
        price REAL NOT NULL,
        quantity REAL NOT NULL,
        first_trade_id INTEGER NOT NULL,
        last_trade_id INTEGER NOT NULL,
        transact_time INTEGER NOT NULL,
        is_buyer_maker BOOLEAN NOT NULL
    )
    """)

    # Ensure the UNIQUE constraint is set for symbol, interval, and open_time
    cursor.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS idx_klines_symbol_interval_open_time 
    ON klines (symbol, interval, open_time)
    """)

    # Convert the klines data by adding interval and symbol information
    klines_with_interval_and_symbol = [
        (
            symbol,
            interval,
            int(kline[0]),           # open_time
            float(kline[1]),         # open
            float(kline[2]),         # high
            float(kline[3]),         # low
            float(kline[4]),         # close
            float(kline[5]),         # volume
            int(kline[6]),           # close_time
            float(kline[7]),         # quote_asset_volume
            int(kline[8]),           # trades
            float(kline[9]),         # taker_buy_base_asset_volume
            float(kline[10]),        # taker_buy_quote_asset_volume
            float(kline[11])         # ignore_column
        ) 
        for kline in klines_data
    ]

    # Insert klines data into the table
    cursor.executemany("""
    INSERT OR REPLACE INTO klines (symbol, interval, open_time, open, high, low, close, volume, close_time, 
                       quote_asset_volume, trades, taker_buy_base_asset_volume, 
                       taker_buy_quote_asset_volume, ignore_column)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, klines_with_interval_and_symbol)

    # Commit the changes and close the connection
    conn.commit()
    conn.close()


def store_aggregated_trades_to_db(aggregated_trades_data, symbol, dbname="klines_data.db"):
    conn = sqlite3.connect(dbname)
    cursor = conn.cursor()
    
    # Ensure the UNIQUE constraint is set for symbol and transact_time
    cursor.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS idx_symbol_transact_time 
    ON aggregated_trades (symbol, transact_time)
    """)

    # Convert the aggregated_trades data by adding symbol information
    aggregated_trades_with_symbol = [
        (
            float(trade[1]),  # agg_trade_id
            trade[0],     # symbol
            float(trade[2]),  # price
            float(trade[3]),  # quantity
            trade[4],     # first_trade_id
            trade[5],     # last_trade_id
            trade[6],     # transact_time
            int(trade[7])      # is_buyer_maker (ensure it's an integer)
        ) 
        for trade in aggregated_trades_data
    ]



    for trade in aggregated_trades_with_symbol[:10]:  # printing the first 10 for inspection
        print(trade)


    # Insert or replace aggregated_trades data into the table
    cursor.executemany("""
    INSERT OR REPLACE INTO aggregated_trades 
    (agg_trade_id, symbol, price, quantity, first_trade_id, 
     last_trade_id, transact_time, is_buyer_maker)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, aggregated_trades_with_symbol)

    # Commit the changes and close the connection
    conn.commit()
    conn.close()


# def store_aggregated_trades_to_db(aggregated_trades_data, symbol, dbname="klines_data.db"):
#     conn = sqlite3.connect(dbname)
#     cursor = conn.cursor()
    
#     # Ensure the UNIQUE constraint is set for symbol, and transact_time
#     cursor.execute("""
#     CREATE UNIQUE INDEX IF NOT EXISTS idx_symbol_transact_time 
#     ON aggregated_trades (symbol, transact_time)
#     """)

#     # Convert the aggregated_trades data by adding symbol and interval information
#     aggregated_trades_with_symbol = [
#         (
#             trade[0],     # agg_trade_id
#             symbol,       # symbol
#             float(trade[1]),  # price
#             float(trade[2]),  # quantity
#             trade[3],     # first_trade_id
#             trade[4],     # last_trade_id
#             trade[5],     # transact_time
#             trade[6]      # is_buyer_maker
#         ) 
#         for trade in aggregated_trades_data
#     ]

#     # Insert or replace aggregated_trades data into the table
#     cursor.executemany("""
#     INSERT OR REPLACE INTO aggregated_trades 
#     (agg_trade_id, symbol, price, quantity, first_trade_id, 
#      last_trade_id, transact_time, is_buyer_maker)
#     VALUES (?, ?, ?, ?, ?, ?, ?, ?)
#     """, aggregated_trades_with_symbol)

#     # Commit the changes and close the connection
#     conn.commit()
#     conn.close()




def print_in_chunks(df, chunk_size=50):
    if len(df) <= chunk_size:
        print(df)
        return

    num_chunks = (len(df) - 1) // chunk_size + 1
    
    for i in range(num_chunks):
        start_idx = i * chunk_size
        end_idx = start_idx + chunk_size
        print("Part {i}")
        print(df[start_idx:end_idx])
        # input("Press Enter to see the next chunk...")  # Wait for the user to press Enter before showing the next chunk

def list_files(directory, prefix="2023-08-18"):
    """
    List all files in the specified directory that start with the given prefix.

    :param directory: The directory to search in.
    :param prefix: The prefix to match filenames against.
    :return: A list of matching filenames.
    """
    files = os.listdir(directory)
    return [f for f in files if f.startswith("BTCUSDT-aggTrades-" + prefix)]

# Use the function
# directory_path = "/opt/works/personal/gpt_analysis/data/futures/BTC_USDT"
# august_files = list_files(directory_path)
# print(august_files)


# if __name__ == "__main__":
#     # filename = ["BTCUSDT-aggTrades-2023-08-16.csv","FUTURE_BTCUSDT_2023-08-18.csv"]
#     # filename = ["BTCUSDT-aggTrades-2023-08-12.csv","BTCUSDT-aggTrades-2023-08-17.csv"]
#     directory_path = "/opt/works/personal/gpt_analysis/data/futures/BTC_USDT"
#     filename = list_files(directory_path)
#     print(filename)

#     for f in filename:
#         path = os.path.join("./data/futures/BTC_USDT/",f)
#         df = read_data(path)

#         interval = '30T'
#         candle_footprint = footprint_candle_agg(df,interval)
#         print("Footprint Candle:")
#         print_in_chunks(candle_footprint)

#         # vp = calculate_volume_profile(df)
#         # print("calculate_volume_profile Candle:")
#         # print_in_chunks(vp)



#     #     vp_chart = calculate_advanced_volume_profile(df)
#     #     print("calculate_advanced_volume_profile Candle:")
#     #     print_in_chunks(vp_chart)

#     # filename = "./data/futures/BTC_USDT/BTCUSDT-bookTicker-2023-08-16.csv"
#     # df = read_bookticker_data(filename)

#     # candle_footprint = calculate_footprint(df,interval)
#     # print("Footprint Candle:", candle_footprint.tail(100))

#         # interval = '30T'
#     # tpo_chart_letters, tpo_chart = calculate_tpo_agg(df)
#     # print("TPO chart:", tpo_chart_letters.tail(100),tpo_chart.tail(100))
