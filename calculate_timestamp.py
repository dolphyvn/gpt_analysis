from datetime import datetime, timedelta
from mysql_connector import *

start_date = datetime(2023, 8, 22)
end_date = start_date + timedelta(days=1)

start_timestamp = int(start_date.timestamp() * 1000)  # Convert to milliseconds
end_timestamp = int(end_date.timestamp() * 1000)  # Convert to milliseconds



data = query_mysql(f"""
    SELECT price, SUM(quantity) AS total_volume 
    FROM aggregated_trades 
	WHERE transact_time >= {start_timestamp}
	AND transact_time < {end_timestamp}
	AND symbol='BTCUSDT'
    GROUP BY price 
    ORDER BY total_volume DESC
""")

poc_price, poc_volume = data[0]
print(f"POC Price: {poc_price}, POC Volume: {poc_volume}")