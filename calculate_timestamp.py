from datetime import datetime, timedelta

start_date = datetime(2023, 6, 3)
end_date = start_date + timedelta(days=1)

start_timestamp = int(start_date.timestamp() * 1000)  # Convert to milliseconds
end_timestamp = int(end_date.timestamp() * 1000)  # Convert to milliseconds
print(start_timestamp)
print(end_timestamp)
print(f"""SELECT * 
	FROM aggregated_trades 
	WHERE transact_time >= {start_timestamp}
	AND transact_time < {end_timestamp}
	LIMIT 10;""")