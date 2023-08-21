from flask import Flask, jsonify, request
import sqlite3
from mysql_connector import *


app = Flask(__name__)

DATABASE_NAME = "klines_data.db"

DEFAULT_PAGE_SIZE = 100

def query_db(query, args=(), one=False):
    conn = sqlite3.connect(DATABASE_NAME)
    cur = conn.cursor().execute(query, args)
    rv = cur.fetchall()
    conn.close()
    return (rv[0] if rv else None) if one else rv


def query_mysql(query, args=(), one=False):
    conn = mysql.connector.connect(**DB_CONFIG)  # <-- Changed connection logic
    cur = conn.cursor()
    cur.execute(query, args)
    rv = cur.fetchall()
    conn.close()
    return (rv[0] if rv else None) if one else rv

@app.route("/api/klines", methods=["GET"])
def get_klines():
    symbol = request.args.get('symbol')
    timeframe = request.args.get('timeframe')
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', DEFAULT_PAGE_SIZE))
    offset = (page - 1) * limit

    if symbol and timeframe:
        data = query_mysql("SELECT * FROM klines WHERE symbol=%s AND timeframe=%s LIMIT %s OFFSET %s", (symbol, timeframe, limit, offset))
    elif symbol:
        data = query_mysql("SELECT * FROM klines WHERE symbol=%s LIMIT %s OFFSET %s", (symbol, limit, offset))
    else:
        data = query_mysql("SELECT * FROM klines LIMIT %s OFFSET %s", (limit, offset))
    

    # Convert data to a list of dictionaries for JSON serialization
    # klines = [{"id": kline[0], "symbol": kline[1], ...} for kline in data]  # expand for all columns
    klines = [
        {
            "id": kline[0],
            "symbol": kline[1],
            "timeframe": kline[2],
            "open_time": kline[3],
            "open": kline[4],
            "high": kline[5],
            "low": kline[6],
            "close": kline[7],
            "volume": kline[8],
            "close_time": kline[9],
            "quote_asset_volume": kline[10],
            "trades": kline[11],
            "taker_buy_base_asset_volume": kline[12],
            "taker_buy_quote_asset_volume": kline[13],
            "ignore_column": kline[14]
        }
        for kline in data
    ]

    return jsonify(klines)


@app.route("/api/aggregated_trades", methods=["GET"])
def get_aggregated_trades():
    symbol = request.args.get('symbol')
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', DEFAULT_PAGE_SIZE))
    offset = (page - 1) * limit

    if symbol:
        data = query_mysql("SELECT * FROM aggregated_trades WHERE symbol=%s LIMIT %s OFFSET %s", (symbol, limit, offset))
    else:
        data = query_mysql("SELECT * FROM aggregated_trades LIMIT %s OFFSET %s", (limit, offset))

    
    # Convert data to a list of dictionaries for JSON serialization
    # aggregated_trades = [{"agg_trade_id": trade[0], "symbol": trade[1], ...} for trade in data]  # expand for all columns
    aggregated_trades = [
        {
            "agg_trade_id": trade[0],
            "symbol": trade[1],
            "price": trade[2],
            "quantity": trade[3],
            "first_trade_id": trade[4],
            "last_trade_id": trade[5],
            "transact_time": trade[6],
            "is_buyer_maker": bool(trade[7])  # Convert integer to boolean for JSON
        }
        for trade in data
    ]

    return jsonify(aggregated_trades)


# Additional function to get the opening and closing price
def get_open_close(symbol, date):
    data = query_mysql("""
        SELECT open, close 
        FROM klines 
        WHERE symbol=%s 
        AND DATE(FROM_UNIXTIME(open_time/1000)) = %s 
        LIMIT 1
    """, (symbol, date))
    if data:
        return {"open": data[0][0], "close": data[0][1]}
    return {"open": None, "close": None}

# Function to get the POC
def get_poc(symbol, date):
    data = query_mysql("""
        SELECT price, SUM(quantity) AS total_volume 
        FROM aggregated_trades 
        WHERE symbol=%s 
        AND DATE(FROM_UNIXTIME(transact_time/1000)) = %s
        GROUP BY price 
        ORDER BY total_volume DESC 
        LIMIT 1
    """, (symbol, date))
    if data:
        return {"poc_price": data[0][0], "poc_volume": data[0][1]}
    return {"poc_price": None, "poc_volume": None}

# Function to get volume and transaction times for each price level
def get_volume_per_price(symbol, date):
    # First, get volume and transaction times for each price level from aggregated_trades
    aggregated_data = query_mysql("""
        SELECT price, GROUP_CONCAT(transact_time) AS transaction_times, SUM(quantity) AS total_volume 
        FROM aggregated_trades 
        WHERE symbol=%s 
        AND DATE(FROM_UNIXTIME(transact_time/1000)) = %s
        GROUP BY price 
        ORDER BY price
    """, (symbol, date))

    result = []
    for d in aggregated_data:
        price = d[0]
        transaction_times = list(map(int, d[1].split(',')))
        volume = d[2]
        
        # Query klines to get the duration for which the price remained at the aggregated trade price
        kline_durations = query_mysql("""
            SELECT SUM(close_time - open_time) 
            FROM klines 
            WHERE symbol=%s 
            AND DATE(FROM_UNIXTIME(close_time/1000)) = %s 
            AND close=%s
        """, (symbol, date, price))

        # Assuming the SQL returns a single row with the sum of the durations
        duration = kline_durations[0][0] if kline_durations else 0

        result.append({
            "price": price,
            "transaction_times": transaction_times,
            "volume": volume,
            "duration": duration  # duration for which the price remained at that level
        })

    return result


@app.route("/api/vp_data", methods=["GET"])
def get_vp_data():
    symbol = request.args.get('symbol')
    date = request.args.get('date')  # expecting date in format YYYY-MM-DD
    
    if not symbol or not date:
        return jsonify({"error": "Both symbol and date are required."}), 400

    open_close = get_open_close(symbol, date)
    poc = get_poc(symbol, date)
    volume_data = get_volume_per_price(symbol, date)

    return jsonify({
        "open": open_close["open"],
        "close": open_close["close"],
        "poc": poc,
        "volume_per_price": volume_data
    })



@app.route("/api/klines_aggregated_trades", methods=["GET"])
def get_klines_agg_data():
    symbol = request.args.get('symbol')
    timeframe = request.args.get('timeframe')
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', DEFAULT_PAGE_SIZE))
    offset = (page - 1) * limit

    if symbol and timeframe:
        klines_data = query_mysql("SELECT * FROM klines WHERE symbol=? AND timeframe=? LIMIT ? OFFSET ?", (symbol, timeframe, limit, offset))
        aggregated_trades_data = query_mysql("SELECT * FROM aggregated_trades WHERE symbol=? LIMIT ? OFFSET ?", (symbol, limit, offset))
    elif symbol:
        klines_data = query_mysql("SELECT * FROM klines WHERE symbol=? LIMIT ? OFFSET ?", (symbol, limit, offset))
        aggregated_trades_data = query_mysql("SELECT * FROM aggregated_trades WHERE symbol=? LIMIT ? OFFSET ?", (symbol, limit, offset))
    else:
        klines_data = query_mysql("SELECT * FROM klines LIMIT ? OFFSET ?", (limit, offset))
        aggregated_trades_data = query_mysql("SELECT * FROM aggregated_trades LIMIT ? OFFSET ?", (limit, offset))
    
    # Convert klines_data to a list of dictionaries
    klines = [
        {
            "id": kline[0],
            "symbol": kline[1],
            "timeframe": kline[2],
            "open_time": kline[3],
            "open": kline[4],
            "high": kline[5],
            "low": kline[6],
            "close": kline[7],
            "volume": kline[8],
            "close_time": kline[9],
            "quote_asset_volume": kline[10],
            "trades": kline[11],
            "taker_buy_base_asset_volume": kline[12],
            "taker_buy_quote_asset_volume": kline[13],
            "ignore_column": kline[14]
        }
        for kline in klines_data
    ]

    # Convert aggregated_trades_data to a list of dictionaries
    aggregated_trades = [
        {
            "agg_trade_id": trade[0],
            "symbol": trade[1],
            "price": trade[2],
            "quantity": trade[3],
            "first_trade_id": trade[4],
            "last_trade_id": trade[5],
            "transact_time": trade[6],
            "is_buyer_maker": bool(trade[7])
        }
        for trade in aggregated_trades_data
    ]

    combined_data = {
        "klines": klines,
        "aggregated_trades": aggregated_trades
    }

    return jsonify(combined_data)



# To run the API
if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0',port=5000)
