import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os



load_dotenv()


# Database Configuration
DB_CONFIG = {
    "host": os.getenv("db_host"),  # or your MySQL server IP/host
    "user": os.getenv("db_user"),
    "password": os.getenv("db_passwd"),
    "database": os.getenv("db_name")
}

def query_mysql(query, args=(), one=False):
    conn = mysql.connector.connect(**DB_CONFIG)  # <-- Changed connection logic
    cur = conn.cursor()
    cur.execute(query, args)
    rv = cur.fetchall()
    conn.close()
    return (rv[0] if rv else None) if one else rv

def create_connection():
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            return connection
    except Error as e:
        print("Error while connecting to MySQL", e)
        return None

def store_klines_to_mysql(klines_data, timeframe, symbol):
    connection = create_connection()
    if not connection:
        return

    cursor = connection.cursor()

    # Create tables if they don't exist
    create_klines_table_query = """
    CREATE TABLE IF NOT EXISTS klines (
        id INT AUTO_INCREMENT PRIMARY KEY,
        symbol VARCHAR(255) NOT NULL,
        timeframe VARCHAR(255) NOT NULL,
        open_time BIGINT NOT NULL,
        open DOUBLE NOT NULL,
        high DOUBLE NOT NULL,
        low DOUBLE NOT NULL,
        close DOUBLE NOT NULL,
        volume DOUBLE NOT NULL,
        close_time BIGINT NOT NULL,
        quote_asset_volume DOUBLE NOT NULL,
        trades BIGINT NOT NULL,
        taker_buy_base_asset_volume DOUBLE NOT NULL,
        taker_buy_quote_asset_volume DOUBLE NOT NULL,
        ignore_column DOUBLE NOT NULL,
        UNIQUE KEY idx_klines_symbol_timeframe_open_time (symbol, timeframe, open_time)
    )
    """
    cursor.execute(create_klines_table_query)

    klines_with_timeframe_and_symbol = [(symbol, timeframe, *kline) for kline in klines_data]

    insert_klines_query = """
    INSERT INTO klines (symbol, timeframe, open_time, open, high, low, close, volume, close_time, 
                        quote_asset_volume, trades, taker_buy_base_asset_volume, 
                        taker_buy_quote_asset_volume, ignore_column)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
    open = VALUES(open),
    high = VALUES(high),
    low = VALUES(low),
    close = VALUES(close),
    volume = VALUES(volume),
    close_time = VALUES(close_time),
    quote_asset_volume = VALUES(quote_asset_volume),
    trades = VALUES(trades),
    taker_buy_base_asset_volume = VALUES(taker_buy_base_asset_volume),
    taker_buy_quote_asset_volume = VALUES(taker_buy_quote_asset_volume),
    ignore_column = VALUES(ignore_column)
    """
    
    # Break data into chunks and use transactions
    chunk_size = 1000
    connection.start_transaction()
    try:
        for i in range(0, len(klines_with_timeframe_and_symbol), chunk_size):
            chunk = klines_with_timeframe_and_symbol[i:i+chunk_size]
            cursor.executemany(insert_klines_query, chunk)
        connection.commit()
    except:
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()


def store_aggregated_trades_to_mysql(aggregated_trades_data, symbol):
    connection = create_connection()
    if not connection:
        return

    cursor = connection.cursor()

    create_aggregated_trades_table_query = """
    CREATE TABLE IF NOT EXISTS aggregated_trades (
        agg_trade_id BIGINT PRIMARY KEY,
        symbol VARCHAR(255) NOT NULL,
        price DOUBLE NOT NULL,
        quantity DOUBLE NOT NULL,
        first_trade_id BIGINT NOT NULL,
        last_trade_id BIGINT NOT NULL,
        transact_time BIGINT NOT NULL,
        is_buyer_maker BOOLEAN NOT NULL,
        UNIQUE KEY idx_symbol_transact_time (symbol, transact_time)
    )
    """
    cursor.execute(create_aggregated_trades_table_query)
    
    aggregated_trades_with_symbol = [(trade[1], symbol, *trade[2:]) for trade in aggregated_trades_data]

    insert_aggregated_trades_query = """
    INSERT INTO aggregated_trades 
    (agg_trade_id, symbol, price, quantity, first_trade_id, last_trade_id, transact_time, is_buyer_maker)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
    price = VALUES(price),
    quantity = VALUES(quantity),
    first_trade_id = VALUES(first_trade_id),
    last_trade_id = VALUES(last_trade_id),
    is_buyer_maker = VALUES(is_buyer_maker)
    """
    
    # Break data into chunks and use transactions
    chunk_size = 1000
    connection.start_transaction()
    try:
        for i in range(0, len(aggregated_trades_with_symbol), chunk_size):
            chunk = aggregated_trades_with_symbol[i:i+chunk_size]
            cursor.executemany(insert_aggregated_trades_query, chunk)
        connection.commit()
    except:
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()
