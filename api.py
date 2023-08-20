from flask import Flask, jsonify, request
import sqlite3

app = Flask(__name__)

DATABASE_NAME = "klines_data.db"

@app.route("/api/klines", methods=["GET"])
def get_klines():
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()

    # Fetching all klines (you can add pagination or filters if needed)
    cursor.execute("SELECT * FROM klines")
    data = cursor.fetchall()

    # Close connection
    conn.close()

    # Convert data to a list of dictionaries for JSON serialization
    klines = [{"id": kline[0], "symbol": kline[1], ...} for kline in data]  # expand for all columns

    return jsonify(klines)


@app.route("/api/aggregated_trades", methods=["GET"])
def get_aggregated_trades():
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()

    # Fetching all aggregated trades (you can add pagination or filters if needed)
    cursor.execute("SELECT * FROM aggregated_trades")
    data = cursor.fetchall()

    # Close connection
    conn.close()

    # Convert data to a list of dictionaries for JSON serialization
    aggregated_trades = [{"agg_trade_id": trade[0], "symbol": trade[1], ...} for trade in data]  # expand for all columns

    return jsonify(aggregated_trades)


@app.route("/api/klines_aggregated_trades", methods=["GET"])
def get_vp_data():
    symbol = request.args.get('symbol')  # Get symbol from query parameters

    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()

    # Fetching klines
    if symbol:
        cursor.execute("SELECT * FROM klines WHERE symbol=?", (symbol,))
    else:
        cursor.execute("SELECT * FROM klines")
    klines_data = cursor.fetchall()

    # Fetching aggregated trades
    if symbol:
        cursor.execute("SELECT * FROM aggregated_trades WHERE symbol=?", (symbol,))
    else:
        cursor.execute("SELECT * FROM aggregated_trades")
    aggregated_trades_data = cursor.fetchall()

    # Close connection
    conn.close()

    # Convert data to a list of dictionaries for JSON serialization
    klines = [{"id": kline[0], "symbol": kline[1], ...} for kline in klines_data]  # expand for all columns
    aggregated_trades = [{"agg_trade_id": trade[0], "symbol": trade[1], ...} for trade in aggregated_trades_data]  # expand for all columns

    # Merge or join data. Here, we're just returning them as separate lists, but you can merge based on your criteria
    combined_data = {
        "klines": klines,
        "aggregated_trades": aggregated_trades
    }

    return jsonify(combined_data)


# To run the API
if __name__ == "__main__":
    app.run(debug=True, port=5000)
