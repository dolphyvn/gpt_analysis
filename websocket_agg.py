import os
import pathlib
import asyncio
from typing import List
from binance import AsyncClient, BinanceSocketManager
from dotenv import load_dotenv
from utils import store_aggregated_trades_to_db
from datetime import datetime
from mysql_connector import store_aggregated_trades_to_mysql

# Define thresholds
iceberg_threshold = 100  # Example: Trades below this size might be iceberg orders
sweep_threshold = 10     # Example: Trades above this size might indicate a sweep order


def detect_orders(trade_data):
    if len(trade_data) < 5:
        return  # Not enough data to detect orders
    print(f"Trade data: {trade_data[0]}")
    # Calculate total trade volume and average trade size
    total_volume = sum(quantity for _, quantity, _ in trade_data)
    avg_trade_size = total_volume / len(trade_data)
    
    # Check for potential iceberg orders
    if avg_trade_size <= iceberg_threshold and total_volume > 0:
        print("Potential Iceberg Order Detected!")
        print(f"Average Trade Size: {avg_trade_size}, Total Volume: {total_volume}")
    
    # Check for potential sweep orders
    if avg_trade_size >= sweep_threshold and total_volume > 0:
        print("Potential Sweep Order Detected!")
        print(f"Average Trade Size: {avg_trade_size}, Total Volume: {total_volume}")



def process_message(msg: dict):
    # Extract data in the correct order
    data = (
        # msg['data']['s'],           # symbol
        # msg['data']['a'],           # agg_trade_id
        float(msg['data']['p']),    # price
        float(msg['data']['q']),    # quantity
        # msg['data']['f'],           # first_trade_id
        # msg['data']['l'],           # last_trade_id
        msg['data']['T'],           # transact_time
        # int(msg['data']['m'])       # is_buyer_maker, convert boolean to integer (0 or 1)
    )
    return data


async def main(symbols: List[str], market: str):
    print(f'Started Collecting Tick Data of {symbols}...({market} market)')

    client = await AsyncClient.create()
    bsm = BinanceSocketManager(client)
    agg_symbol = [f"{s}@aggTrade" for s in symbols]

    aggregated_trades = []

    if market == "future":
        async with bsm.futures_multiplex_socket(agg_symbol) as socket:
            while True:
                res = await socket.recv()
                # print(res)
                trade_data = process_message(res)
                # print(trade_data)
                aggregated_trades.append(trade_data)
                
                # You can set a condition to store data after accumulating, say, 100 trades.
                if len(aggregated_trades) > 10:
                    if os.getenv('storage') == 'sqlite3':
                        store_aggregated_trades_to_db(aggregated_trades,trade_data[0])
                        aggregated_trades = []

                    if os.getenv('storage') == 'mysql':
                        store_aggregated_trades_to_mysql(aggregated_trades,trade_data[0])
                        aggregated_trades = []
                    else:
                        print("save to csv")
                        # print(aggregated_trades)
                        detect_orders(aggregated_trades)
    else:
        async with bsm.multiplex_socket(symbols) as socket:
            while True:
                res = await socket.recv()
                trade_data = process_message(res)
                print(trade_data)
                aggregated_trades.append(trade_data)
                
                # You can set a condition to store data after accumulating, say, 100 trades.
                if len(aggregated_trades) > 10:
                    if os.getenv('storage') == 'sqlite3':
                        store_aggregated_trades_to_db(aggregated_trades,trade_data[0])
                        aggregated_trades = []

                    if os.getenv('storage') == 'mysql':
                        store_aggregated_trades_to_mysql(aggregated_trades,trade_data[0])
                        aggregated_trades = []
                    else:
                        print("save to csv")


if __name__ == "__main__":
    load_dotenv()

    symbol_list = [s.lower().strip() for s in os.getenv("symbols").split(",")]
    market_type = os.getenv("market").lower().strip()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(symbol_list, market_type))
