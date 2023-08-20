
# Trading Data Analysis with GPT-4 Integration

This repository is designed to facilitate trading analysis using advanced techniques like Volume Profile (VP), Footprint data, TPO, and Elliott Waves. It integrates with the GPT-4 model via OpenAI to provide dynamic insights and complex analysis with simplicity and clarity.

## Project Structure

-   `download.py`: Script to download aggregated trading data. You can specify the desired dates within the code.
-   `websocket_agg.py`: Fetches aggregated trading data in real-time and stores it in the `aggregated_trades` table.
-   `websocket_klines.py`: Downloads klines trading data based on intervals and pairs loaded from `.env` files. Data is stored in the `klines` table.
-   `api.py`: A Flask-based API to expose the data stored in the two tables: `klines` and `aggregated_trades`.
-   `utils.py`: Contains utility functions that can be imported for analysis purposes.
-   `main.py`: A script to run examples of VP, Footprint, and TPO calculations, providing outputs for these functions.

## Instructions

1.  **Setting Up**:
    
    -   Clone the repository.
    -   Install the required packages using `pip install -r requirements.txt`.
    -   Make sure to set your environment variables in a `.env` file for the `websocket_klines.py`.
2.  **Downloading Data**:
    
    -   Use `download.py` to fetch historical aggregated trading data.
    -   Run `websocket_agg.py` and `websocket_klines.py` to stream real-time data and store it in their respective tables.
3.  **API Usage**:
    
	The project provides a Flask-based API for accessing the stored trading data.

	 Starting the API:

	Start the Flask server using the command:

	bashCopy code

	`python api.py` 

	This will run the server on `http://localhost:5000/`.

	Querying the API:

	Here's a quick guide on how to access the data:

	1.  **klines Endpoint**:
	    
	    -   **Fetch All Klines Data**:
	        
	        bashCopy code
	        
	        `GET http://localhost:5000/api/klines` 
	        
	    -   **Filter by Symbol**:
	        
	        bashCopy code
	        
	        `GET http://localhost:5000/api/klines?symbol=BTCUSD` 
	        
	    -   **Filter by Symbol and Interval**:
	        
	        bashCopy code
	        
	        `GET http://localhost:5000/api/klines?symbol=BTCUSD&interval=1h` 
	        
	2.  **aggregated_trades Endpoint**:
	    
	    -   **Fetch All Aggregated Trades Data**:
	        
	        bashCopy code
	        
	        `GET http://localhost:5000/api/aggregated_trades` 
	        
	    -   **Filter by Symbol**:
	        
	        bashCopy code
	        
	        `GET http://localhost:5000/api/aggregated_trades?symbol=BTCUSD` 
	        
	3.  **klines_aggregated_trades Endpoint**:
	    
	    -   **Fetch Combined Data of Klines and Aggregated Trades**:
	        
	        bashCopy code
	        
	        `GET http://localhost:5000/api/klines_aggregated_trades` 
	        
	    -   **Filter by Symbol**:
	        
	        bashCopy code
	        
	        `GET http://localhost:5000/api/klines_aggregated_trades?symbol=BTCUSD` 
	        
	    -   **Filter by Symbol and Interval**:
	        
	        bashCopy code
	        
	        `GET http://localhost:5000/api/klines_aggregated_trades?symbol=BTCUSD&interval=1h` 
	        

	Each endpoint supports pagination using `page` and `limit` parameters. For instance:

	bashCopy code

	`GET http://localhost:5000/api/klines?page=2&limit=50` 

	This would fetch the second page of klines data with a limit of 50 records per page.
4.  **Analysis with GPT-4**:
    
    -   Use the pre-configured prompts in the repository or craft custom prompts to get insights using the OpenAI's GPT-4 model.
    -   For large data sets or detailed analysis, use the specific instructions provided for breaking data into parts and sending to the GPT model.

## Analysis Prompts

1.  **Advanced Trading Assistance**: This chatbot assistant is geared towards advanced trading techniques, including trend analysis, Volume Profile, TPO, Footprint data, and Elliott Waves. It will answer questions, provide insights, and complete tasks related to trading, always striving for simplicity and clarity.
2.  **Volume Spread Analysis (VSA) & Volume Profile (VP)**: This analysis will give a short and concise verdict like "bullish" or "bearish" based on the data provided.
3.  **Large Data Handling**: When sending large amounts of data, break it into manageable parts. Indicate completion by saying "all done" to get an aggregated response.

## Integration with GPT-4

The project seamlessly integrates with OpenAI's GPT-4 model to provide dynamic and in-depth insights into trading analysis. By leveraging the massive knowledge and predictive capabilities of GPT-4, this tool aims to be a valuable asset for traders and enthusiasts.

## Contributing

Contributions, issues, and feature requests are welcome. Feel free to check the issues page if you want to contribute.