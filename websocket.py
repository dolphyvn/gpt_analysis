import os
import pathlib
import asyncio
from typing import List
from binance import AsyncClient, BinanceSocketManager
from dotenv import load_dotenv
from loguru import logger

from datetime import datetime

def set_logger(data_path: str, symbol: str, market: str):
    current_time = datetime.now()
    formatted_time = current_time.strftime('%Y-%m-%d')  # Gives format like '2023-08-17_14'
    filename = f"{data_path}{market}_{symbol}_{formatted_time}.csv"
    
    # Check if the file already exists, if not write the header
    if not os.path.exists(filename):
        with open(filename, 'w') as file:
            file.write("agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker\n")

    logger.add(filename, rotation=os.getenv("rotation", "12:00"), format="{message}", compression="zip")

# def set_logger(data_path: str, symbol: str, market: str):
#     # logger.add(f"{data_path}{market}_{symbol}" + "_{time}.csv", rotation=os.getenv("rotation", "12:00"),
#     #            format="{message}", compression="zip", filter=lambda rec: rec["extra"]["task"] == f"{market}{symbol}")
#     logger.add(f"{data_path}{market}_{symbol}" + "_{time}.csv", rotation=os.getenv("rotation", "12:00"),
#                format="{message}", compression="zip")


def process_message(msg: dict, market: str):
    # aggTrade format
    # agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker

    columns = ['a', 'p', 'q', 'f', 'l', 'T', 'm']
    data = ",".join([str(msg[col]) for col in columns])
    data_logger = logger.bind(task=f"{market}{msg['a']}")
    data_logger.info(data)
    print(f"Logged data for {market}{msg['a']}")



async def main(symbols: List[str], market: str):
    print(f'Started Collecting Tick Data of {symbols}...({market} market)')
    client = await AsyncClient.create()
    bsm = BinanceSocketManager(client)
    symbols = [f"{s}@aggTrade" for s in symbols]

    if market == "future":
        async with bsm.futures_multiplex_socket(symbols) as socket:
            while True:
                res = await socket.recv()
                print(res)
                process_message(res['data'], market)
    else:
        async with bsm.multiplex_socket(symbols) as socket:
            while True:
                res = await socket.recv()
                process_message(res['data'], market)


if __name__ == "__main__":
    load_dotenv()
    logger.remove()
    data_dir = os.path.join(pathlib.Path().resolve(), 'data/')

    symbol_list = [s.lower().strip() for s in os.getenv("symbols").split(",")]
    market_type = os.getenv("market").lower().strip()
    assert market_type in ["future", "spot"]

    for s in symbol_list:
        data_dir = os.path.join(pathlib.Path().resolve(), 'data/futures/',s.upper().replace("USDT", "_USDT/"))
        set_logger(data_dir, s.upper(), market_type.upper())

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(symbol_list, market_type))
