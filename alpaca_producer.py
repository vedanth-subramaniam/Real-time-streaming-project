import alpaca_trade_api as tradeapi
from kafka import KafkaProducer
import json
import time

# Alpaca API credentials
API_KEY = 'PKB3JIVDNKP8BATDIA8T'
API_SECRET = 'YcifqMwpNFlLRYupEPBSfzPKGzNUbTVOOXewbiRG'
BASE_URL = 'https://paper-api.alpaca.markets'

# Kafka setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Alpaca API client
api = tradeapi.REST(API_KEY, API_SECRET, BASE_URL, api_version='v2')

# Function to fetch and send data
def fetch_and_produce():
    while True:
        try:
            # Fetch data from Alpaca (using IEX data)
            bars = api.get_bars('AAPL', tradeapi.TimeFrame.Minute, start='2023-05-10', end='2023-05-11', feed='iex').df
            print(f"Fetched: {bars}")
            # Produce data to Kafka
            for index, bar in bars.iterrows():
                data = {
                    'time': index.isoformat(),
                    'open': bar['open'],
                    'high': bar['high'],
                    'low': bar['low'],
                    'close': bar['close'],
                    'volume': bar['volume']
                }
                producer.send('alpaca-data', value=data)
                print(f"Produced: {data}")
            time.sleep(60)  # Fetch data every minute
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(60)

if __name__ == "__main__":
    fetch_and_produce()