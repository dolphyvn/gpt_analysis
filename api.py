from flask import Flask, jsonify, request, Response
import sqlite3
from mysql_connector import *
from math import ceil
from polygon_forex import get_data
import json
from io import StringIO
import pandas as pd
import csv


app = Flask(__name__)

DATABASE_NAME = "klines_data.db"

DEFAULT_PAGE_SIZE = 100

# def query_db(query, args=(), one=False):
#     conn = sqlite3.connect(DATABASE_NAME)
#     cur = conn.cursor().execute(query, args)
#     rv = cur.fetchall()
#     conn.close()
#     return (rv[0] if rv else None) if one else rv

def get_total_pages(table_name, limit, where_clause="", where_params=()):
    count_query = f"SELECT COUNT(*) FROM {table_name} {where_clause}"
    total_rows = query_mysql(count_query, where_params)[0][0]
    return ceil(total_rows / limit)

def write_to_json(json_data):

    # JSON file name
    json_file_name = json_data['Ticker'] + ".json"

    # Check if JSON file exists
    if os.path.exists(json_file_name):
        # Read existing JSON file into a list
        with open(json_file_name, 'r') as f:
            existing_data = json.load(f)
            
        # Remove dictionary with the same 'bar' value if exists
        existing_data = [item for item in existing_data if item.get('timestamp') != json_data['timestamp']]
    else:
        existing_data = []

    # Add the new data to the list
    existing_data.append(json_data)

    # Write back to JSON file
    with open(json_file_name, 'w') as f:
        json.dump(existing_data, f, indent=4)

def write_to_csv(json_data):

    # CSV file name
    csv_file_name = json_data['Ticker'] + ".csv"

    # Check if CSV file exists
    if os.path.exists(csv_file_name):
        # Read existing CSV into a list of dictionaries
        with open(csv_file_name, mode='r', newline='') as f:
            reader = csv.DictReader(f)
            existing_data = [row for row in reader]
            
        # Remove row with the same 'timestamp' value if exists
        existing_data = [row for row in existing_data if int(row.get('timestamp', 0)) != json_data['timestamp']]
    else:
        existing_data = []

    # Add the new flattened data to the list
    existing_data.append(json_data)

    # Sort by 'timestamp' (optional)
    existing_data.sort(key=lambda x: int(x.get('timestamp', 0)))

    # Write back to CSV
    with open(csv_file_name, mode='w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=json_data.keys())
        writer.writeheader()
        for row in existing_data:
            writer.writerow(row)

@app.route('/api/atas_test', methods=['POST', 'GET'])
def atas_test():
    if request.method == 'POST':
        json_data = request.json
        del json_data['ticks']
        for js in json_data['candlefootprint']:
            del js['Between']
            del js['Ticks']
            del js['Time']
        # del json_data['candlefootprint']
        # Find hvn and lvn from candlefootprint data

        # Initialize variables to hold the high and low volume nodes
        high_volume_node = None
        low_volume_node = None

        # Initialize variables to hold the highest and lowest volumes
        highest_volume = float("-inf")  # Use negative infinity as a starting point for comparison
        lowest_volume = float("inf")  # Use positive infinity as a starting point for comparison

        # Loop through the data to find the high and low volume nodes
        for node in json_data['candlefootprint']:
            # Calculate and add Delta to each node
            node['Delta'] = node['Bid'] - node['Ask']
            del node['Bid']
            del node['Ask']
        # Loop through the data to find the high and low volume nodes
        for node in json_data['candlefootprint']:
            if node["Volume"] > highest_volume:
                highest_volume = node["Volume"]
                high_volume_node = node
            if node["Volume"] < lowest_volume:
                lowest_volume = node["Volume"]
                low_volume_node = node

        print("High Volume Node:", high_volume_node)
        print("Low Volume Node:", low_volume_node)
        # add these back to json_data
        json_data['hvn'] = high_volume_node
        json_data['lvn'] = low_volume_node

        del json_data['open']
        del json_data['high']
        del json_data['low']
        del json_data['close']
        del json_data['bar']
        del json_data['candlefootprint']

        del json_data['openBidAsk']['Ticks']
        del json_data['openBidAsk']['Between']
        del json_data['openBidAsk']['Time']

        json_data['openBidAsk']['Delta'] = json_data['openBidAsk']['Bid'] - json_data['openBidAsk']['Ask']
        json_data['highBidAsk']['Delta'] = json_data['highBidAsk']['Bid'] - json_data['highBidAsk']['Ask']
        json_data['lowBidAsk']['Delta'] = json_data['lowBidAsk']['Bid'] - json_data['lowBidAsk']['Ask']
        json_data['closeBidAsk']['Delta'] = json_data['closeBidAsk']['Bid'] - json_data['closeBidAsk']['Ask']
        
        json_data['vpoc']['Delta'] = json_data['vpoc']['Bid'] - json_data['vpoc']['Ask']
        json_data['tpoc']['Delta'] = json_data['tpoc']['Bid'] - json_data['tpoc']['Ask']    
        # json_data['vpoc']['Delta'] = json_data['vpoc']['Bid'] - json_data['vpoc']['Ask']    

        del json_data['vpoc']['Bid']
        del json_data['vpoc']['Ask']        

        del json_data['tpoc']['Bid']
        del json_data['tpoc']['Ask']  

        del json_data['tickpoc']
        del json_data['bidpoc']
        del json_data['askpoc']
        
        del json_data['openBidAsk']['Bid']
        del json_data['openBidAsk']['Ask']
        del json_data['highBidAsk']['Bid']
        del json_data['highBidAsk']['Ask']

        del json_data['lowBidAsk']['Bid']
        del json_data['lowBidAsk']['Ask']
        del json_data['closeBidAsk']['Bid']
        del json_data['closeBidAsk']['Ask']
        
        del json_data['highBidAsk']['Ticks']
        del json_data['highBidAsk']['Between']
        del json_data['highBidAsk']['Time']

        del json_data['lowBidAsk']['Ticks']
        del json_data['lowBidAsk']['Between']
        del json_data['lowBidAsk']['Time']

        del json_data['closeBidAsk']['Ticks']
        del json_data['closeBidAsk']['Between']
        del json_data['closeBidAsk']['Time']

        del json_data['vpoc']['Ticks']
        del json_data['vpoc']['Between']
        del json_data['vpoc']['Time']

        # del json_data['tickpoc']['Ticks']
        # del json_data['tickpoc']['Between']
        # del json_data['tickpoc']['Time']

        del json_data['tpoc']['Ticks']
        del json_data['tpoc']['Between']
        del json_data['tpoc']['Time']

        # del json_data['askpoc']['Ticks']
        # del json_data['askpoc']['Between']
        # del json_data['askpoc']['Time']

        # del json_data['bidpoc']['Ticks']
        # del json_data['bidpoc']['Between']
        # del json_data['bidpoc']['Time']
        print(json_data)


        flattened_data = flatten(json_data)

        write_to_json(json_data)
        # write_to_csv(flattened_data)

        return "POST"
    elif request.method == 'GET':
        return "Get"



@app.route('/api/atas_data', methods=['POST', 'GET'])
def atas_data():
    try:
        connection = create_connection()
        if request.method == 'POST':
            data = request.json
            
            ticker = data['Ticker']
            timeframe = data['tf']
            bar = data['Bar']
            timestamp = data['Timestamp']
            last_trade_time = data['LastTradeTime']
            open_price = data['Open']
            high = data['High']
            low = data['Low']
            close = data['Close']
            volume = data['Volume']
            delta = data['Delta']
            bid = data['Bid']
            ask = data['Ask']
            ticks = data['Ticks']
            max_delta = data['MaxDelta']
            min_delta = data['MinDelta']
            max_oi = data['MaxOI']
            min_oi = data['MinOI']

            

            with connection.cursor() as cursor:
                sql = ("INSERT INTO `financial_data` "
                       "(`TimeFrame`,`Ticker`, `Bar`, `Timestamp`, `LastTradeTime`, `Open`, `High`, `Low`, `Close`, `Volume`, "
                       "`Delta`, `Bid`, `Ask`, `Ticks`, `MaxDelta`, `MinDelta`, `MaxOI`, `MinOI`) "
                       "VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
                cursor.execute(sql, (timeframe,ticker, bar, timestamp, last_trade_time, open_price, high, low, close, volume, 
                                    delta, bid, ask, ticks, max_delta, min_delta, max_oi, min_oi))
                connection.commit()


            return jsonify({"status": "success"}), 200
        elif request.method == 'GET':
            timeframe = request.args.get('timeframe', None)
            ticker = request.args.get('ticker', None)

            query_parameters = []
            sql_query = "SELECT * FROM `financial_data` WHERE 1=1"

            if timeframe:
                sql_query += " AND `TimeFrame` = %s"
                query_parameters.append(timeframe)
            
            if ticker:
                sql_query += " AND `Ticker` = %s"
                query_parameters.append(ticker)

            with connection.cursor() as cursor:
                cursor.execute(sql_query, query_parameters)
                result = cursor.fetchall()

            return jsonify({"status": "success", "data": result}), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        if connection:
            connection.close()


@app.route("/api/forex", methods=["GET"])
def get_forex():
    symbol = request.args.get('symbol')
    timeframe = request.args.get('timeframe')
    lookback =  request.args.get('lookback')
    fm = request.args.get('fm')
    prompt = request.args.get('prompt')
    data = get_data(symbol,timeframe,lookback)

    if fm == 'html':
        # Convert the CSV data to DataFrame
        csv_data = StringIO(data)
        df = pd.read_csv(csv_data)

        # Convert DataFrame to HTML table
        html_table = df.to_html(index=False)

        # Return as HTML response
        return Response(html_table, mimetype="text/html")
    else:
        if prompt:
            prompt_message = """I want you to act as a trading expert, 
            your knowledge will focus on all trading technic, 
            like TPO, VP, VSA, DOM, price action, and most importantly the auction theory, 
            this is very important when doing any trend or prediction of the price, 
            We will send you some data, 
            you should go ahead and conduct an trend analysis using VP,VSA with auction theory as foundation. 
            And your anwser should be short, only verdict, like bullish or bearish. Here is the data:"""
            html_data = f"<pre>{prompt_message}</pre> <pre>{data}</pre>"
            # response = Response(data, mimetype='text/csv')
            # response.headers["Content-Type"] = "text/csv"
            # response.headers["Content-Disposition"] = "inline; filename=data.csv"
            
            return Response(html_data, mimetype="text/html")
        else:
            html_data = f"<pre>{data}</pre>"
            # response = Response(data, mimetype='text/csv')
            # response.headers["Content-Type"] = "text/csv"
            # response.headers["Content-Disposition"] = "inline; filename=data.csv"
            
            return Response(html_data, mimetype="text/html")            

@app.route('/api/atas', methods=['POST'])
def atas_receive_data():
    # Extract data from POST request
    data = request.json
    pretty_json = json.dumps(json.loads(json_data), indent=4)
    print(pretty_json)
    # Here you can process or store the data as needed.
    # For the purpose of this example, we'll simply return the received data.
    # print(jsonify(data))
    return jsonify(data), 200

@app.route('/api/bm', methods=['POST'])
def bm_receive_data():
    # Extract data from POST request
    data = request.json
    pretty_json = json.dumps(json.loads(json_data), indent=4)
    print(pretty_json)
    # Here you can process or store the data as needed.
    # For the purpose of this example, we'll simply return the received data.
    # print(jsonify(data))
    return jsonify(data), 200

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

    where_clause = ""
    where_params = ()
    if symbol and timeframe:
        where_clause = "WHERE symbol=%s AND timeframe=%s"
        where_params = (symbol, timeframe)
    elif symbol:
        where_clause = "WHERE symbol=%s"
        where_params = (symbol,)

    total_pages = get_total_pages("klines", limit, where_clause, where_params)

    return jsonify({
        "total_pages": total_pages,
        "klines": klines
    })


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

    where_clause = ""
    where_params = ()
    if symbol:
        where_clause = "WHERE symbol=%s"
        where_params = (symbol,)

    total_pages = get_total_pages("aggregated_trades", limit, where_clause, where_params)

    return jsonify({
        "total_pages": total_pages,
        "aggregated_trades": aggregated_trades
    })


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
def get_volume_per_price_(symbol, date):
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


def get_volume_per_price(symbol, date, limit, offset, price_difference=5.0):
    # Adjusted query to fully comply with ONLY_FULL_GROUP_BY SQL mode and added LIMIT/OFFSET
    aggregated_data = query_mysql("""
        SELECT 
            FLOOR(price / %s) * %s AS price_bin_start, 
            (FLOOR(price / %s) + 1) * %s AS price_bin_end,
            GROUP_CONCAT(transact_time) AS transaction_times, 
            SUM(quantity) AS total_volume 
        FROM aggregated_trades 
        WHERE symbol=%s 
        AND DATE(FROM_UNIXTIME(transact_time/1000)) = %s
        GROUP BY price_bin_start, price_bin_end
        ORDER BY price_bin_start
        LIMIT %s OFFSET %s
    """, (price_difference, price_difference, price_difference, price_difference, symbol, date, limit, offset))

    result = []
    for d in aggregated_data:
        price_bin_start = d[0]
        price_bin_end = d[1]
        transaction_times = list(map(int, d[2].split(',')))
        volume = d[3]

        result.append({
            "price_bin_start": price_bin_start,
            "price_bin_end": price_bin_end,
            "transaction_times": transaction_times,
            "volume": volume
        })

    return result

# Flatten the nested JSON data
def flatten(json_data, parent_key='', sep='_'):
    items = {}
    for k, v in json_data.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten(v, new_key, sep=sep))
        else:
            items[new_key] = v
    return items

@app.route("/api/vp_data", methods=["GET"])
def get_vp_data():
    symbol = request.args.get('symbol')
    date = request.args.get('date')  # expecting date in format YYYY-MM-DD
    page = int(request.args.get('page', 1))  # default to first page
    limit = int(request.args.get('limit', 10))  # default limit to 10 items per page
    
    if not symbol or not date:
        return jsonify({"error": "Both symbol and date are required."}), 400

    open_close = get_open_close(symbol, date)
    poc = get_poc(symbol, date)
    volume_data = get_volume_per_price(symbol, date, limit, (page - 1) * limit)

    where_clause = "WHERE symbol=%s AND DATE(FROM_UNIXTIME(transact_time/1000)) = %s"
    where_params = (symbol, date)
    total_pages = get_total_pages("aggregated_trades", limit, where_clause, where_params)

    return jsonify({
        "total_pages": total_pages,
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
    app.run(debug=True, host='0.0.0.0',port=8080)
