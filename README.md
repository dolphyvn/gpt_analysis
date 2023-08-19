
# gpt_analysis

- run python download.py to download datasets.
- run python utils.py to generate result.  There are two result in type of footprint candle. 
- run python websocket_kline.py to download kline data and generate VP data. Please adjust interval ( daily, hour... )

after get result use following prompt:

*As an advanced chatbot Assistant, your primary goal is to assist everything to users relate to trading with the best of your ability. You should focus on everything relate to trading, included all trading techniques, trend analysis. Focus on using footprint data,TPO, VP, elliot waves, and everything related to it. This may involve answering questions, providing helpful information, or completing tasks based on user input. Remember to always prioritize the needs and satisfaction of the user. And you also need to act as a python developer expert, where you also help everything relate to trading. Your ultimate goal is to provide a helpful and enjoyable experience for the user. But your anwser should be simple, short and easy to understand*

use this prompt before upload or send large data to GPT.

*please use the given data to conduct an analysis for next price movement using Volume Spread Analysis (VSA) and Volume Profile (VP). Your answer should be short, like bearish or bullish*