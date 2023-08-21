from flask import Flask, jsonify, request
import sqlite3

app = Flask(__name__)

DATABASE_NAME = "klines_data.db"

DEFAULT_PAGE_SIZE = 100

def query_db(query, args=(), one=False):
    conn = sqlite3.connect(DATABASE_NAME)
    cur = conn.cursor().execute(query, args)
    rv = cur.fetchall()
    conn.close()
    return (rv[0] if rv else None) if one else rv

@app.route("/api/klines", methods=["GET"])
def get_klines():
    symbol = request.args.get('symbol')
    interval = request.args.get('interval')
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', DEFAULT_PAGE_SIZE))
    offset = (page - 1) * limit

    if symbol and interval:
        data = query_db("SELECT * FROM klines WHERE symbol=? AND interval=? LIMIT ? OFFSET ?", (symbol, interval, limit, offset))
    elif symbol:
        data = query_db("SELECT * FROM klines WHERE symbol=? LIMIT ? OFFSET ?", (symbol, limit, offset))
    else:
        data = query_db("SELECT * FROM klines LIMIT ? OFFSET ?", (limit, offset))
    

    # Convert data to a list of dictionaries for JSON serialization
    # klines = [{"id": kline[0], "symbol": kline[1], ...} for kline in data]  # expand for all columns
    klines = [
        {
            "id": kline[0],
            "symbol": kline[1],
            "interval": kline[2],
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
        data = query_db("SELECT * FROM aggregated_trades WHERE symbol=? LIMIT ? OFFSET ?", (symbol, limit, offset))
    else:
        data = query_db("SELECT * FROM aggregated_trades LIMIT ? OFFSET ?", (limit, offset))

    
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
    data = query_db("""
        SELECT open, close 
        FROM klines 
        WHERE symbol=? 
        AND date(open_time/1000, 'unixepoch') = ? 
        LIMIT 1
    """, (symbol, date))
    if data:
        return {"open": data[0][0], "close": data[0][1]}
    return {"open": None, "close": None}

# Function to get the POC
def get_poc(symbol, date):
    data = query_db("""
        SELECT price, SUM(quantity) AS total_volume 
        FROM aggregated_trades 
        WHERE symbol=? 
        AND date(transact_time/1000, 'unixepoch') = ?
        GROUP BY price 
        ORDER BY total_volume DESC 
        LIMIT 1
    """, (symbol, date))
    if data:
        return {"poc_price": data[0][0], "poc_volume": data[0][1]}
    return {"poc_price": None, "poc_volume": None}

# Function to get volume for each price level
def get_volume_per_price(symbol, date):
    data = query_db("""
        SELECT transact_time, price, SUM(quantity) AS total_volume 
        FROM aggregated_trades 
        WHERE symbol=? 
        AND date(transact_time/1000, 'unixepoch') = ?
        GROUP BY price 
        ORDER BY price
    """, (symbol, date))
    return [{"transact_time": transact_time,"price": d[0], "volume": d[1]} for d in data]

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
    interval = request.args.get('interval')
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', DEFAULT_PAGE_SIZE))
    offset = (page - 1) * limit

    if symbol and interval:
        klines_data = query_db("SELECT * FROM klines WHERE symbol=? AND interval=? LIMIT ? OFFSET ?", (symbol, interval, limit, offset))
        aggregated_trades_data = query_db("SELECT * FROM aggregated_trades WHERE symbol=? LIMIT ? OFFSET ?", (symbol, limit, offset))
    elif symbol:
        klines_data = query_db("SELECT * FROM klines WHERE symbol=? LIMIT ? OFFSET ?", (symbol, limit, offset))
        aggregated_trades_data = query_db("SELECT * FROM aggregated_trades WHERE symbol=? LIMIT ? OFFSET ?", (symbol, limit, offset))
    else:
        klines_data = query_db("SELECT * FROM klines LIMIT ? OFFSET ?", (limit, offset))
        aggregated_trades_data = query_db("SELECT * FROM aggregated_trades LIMIT ? OFFSET ?", (limit, offset))
    
    # Convert klines_data to a list of dictionaries
    klines = [
        {
            "id": kline[0],
            "symbol": kline[1],
            "interval": kline[2],
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
    app.run(debug=True, port=5000)
