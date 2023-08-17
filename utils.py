import pandas as pd
from collections import defaultdict
from datetime import datetime


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


if __name__ == "__main__":
    filename = "./data/futures/BTC_USDT/BTCUSDT-aggTrades-2023-08-16.csv"
    df = read_data(filename)

    candle_footprint = footprint_candle_agg(df,'30T')
    print("5T Footprint Candle:", candle_footprint.tail(50))
    # vp = calculate_volume_profile(df)
    # print("Volume Profile (daily value area):\n", vp)

    # daily_footprint = calculate_daily_footprint(df,10.0)
    # print("Daily Footprint Candle:", daily_footprint.tail(50))

    filename = "./data/futures/BTC_USDT/BTCUSDT-bookTicker-2023-08-16.csv"
    df = read_bookticker_data(filename)

    candle_footprint = calculate_footprint(df,'30T')
    print("30T Footprint Candle:", candle_footprint.tail(50))
    # tpo = calculate_TPO(df)
    # print("TPO:\n", tpo)
    
