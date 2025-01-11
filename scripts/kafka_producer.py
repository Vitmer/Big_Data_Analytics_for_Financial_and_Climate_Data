import yfinance as yf
import osmnx as ox
from confluent_kafka import Producer
import json
import time
import logging
import redis
import asyncio
from logging.handlers import RotatingFileHandler

# Setting up log rotation
log_handler = RotatingFileHandler('logs/producer.log', maxBytes=5*1024*1024, backupCount=3)
log_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_handler.setFormatter(formatter)

# Adding console output handler
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(formatter)

logging.getLogger().addHandler(log_handler)
logging.getLogger().addHandler(console)
logging.basicConfig(level=logging.INFO)

# Set up Redis for preventing duplicate messages
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

# Kafka configuration with 'acks' for reliability and 'linger.ms' for performance optimization
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'data-producer',
    'acks': 'all',  # Ensures guaranteed message delivery
    'linger.ms': 10  # Wait 10 ms to batch messages for better throughput
}
producer = Producer(kafka_config)

# Using ThreadPoolExecutor for asynchronous requests
executor = ThreadPoolExecutor(max_workers=3)

# Function to send messages to Kafka with duplication check through Redis
async def send_to_kafka(topic, message):
    """Send a message to Kafka with duplication check"""
    try:
        # Determine a unique key for each message
        if topic == "finance-data":
            message_key = f"{message['ticker']}_{message['date']}_{message['volume']}"  # Added volume for uniqueness
        elif topic == "weather-data":
            message_key = f"{message['city']}_{message['date']}"
        elif topic == "osm-data":
            message_key = f"{message['place']}_{message['date']}"
        else:
            logging.error(f"Unknown topic: {topic}")
            return

        # Check if the message has already been sent (using Redis)
        if redis_client.get(message_key) is None:
            # If the message is unique, send it to Kafka
            producer.produce(topic, value=json.dumps(message))
            producer.flush()
            redis_client.set(message_key, 'sent')  # Save the key in Redis
            logging.info(f"Sent to Kafka: {message}")
        else:
            logging.info(f"Duplicate message skipped: {message}")
    except Exception as e:
        logging.error(f"Failed to send message to Kafka: {e}")

# Function to fetch data from Yahoo Finance
def fetch_yahoo_finance_data(ticker):
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="1d")
        latest_data = hist.tail(1).reset_index().iloc[0]
        data = {
            "source": "Yahoo Finance",
            "ticker": ticker,
            "date": str(latest_data["Date"]),
            "open": float(latest_data["Open"]),
            "close": float(latest_data["Close"]),
            "high": float(latest_data["High"]),
            "low": float(latest_data["Low"]),
            "volume": int(latest_data["Volume"])
        }
        logging.info(f"Fetched Yahoo Finance data: {data}")
        return data
    except Exception as e:
        logging.error(f"Error fetching Yahoo Finance data for {ticker}: {e}")
        return None

# Function to fetch data from OpenWeatherMap
def fetch_openweather_data():
    try:
        data = {
            "source": "OpenWeatherMap",
            "city": "London",
            "temperature": 280.32,  # Static example temperature in Kelvin
            "humidity": 81,         # Static example humidity
            "weather": "Clear"      # Static example weather description
        }
        logging.info(f"Fetched OpenWeatherMap data: {data}")
        return data
    except Exception as e:
        logging.error(f"Error fetching OpenWeatherMap data: {e}")
        return None

# Function to fetch data from OpenStreetMap
def fetch_osm_data(place):
    try:
        graph = ox.graph_from_place(place, network_type='drive')
        edges = ox.graph_to_gdfs(graph, nodes=False)
        data = {
            "source": "OpenStreetMap",
            "place": place,
            "edges_count": int(len(edges)),
            "average_length": float(edges["length"].mean())
        }
        logging.info(f"Fetched OpenStreetMap data: {data}")
        return data
    except Exception as e:
        logging.error(f"Error fetching OpenStreetMap data for {place}: {e}")
        return None

# Main process
async def main():
    # Kafka topics
    finance_topic = "finance-data"
    weather_topic = "weather-data"
    osm_topic = "osm-data"

    while True:
        try:
            # Asynchronously fetch data and send to Kafka
            finance_data = fetch_yahoo_finance_data("AAPL")
            if finance_data:
                await send_to_kafka(finance_topic, finance_data)

            weather_data = fetch_openweather_data()
            if weather_data:
                await send_to_kafka(weather_topic, weather_data)

            osm_data = fetch_osm_data("New York, USA")
            if osm_data:
                await send_to_kafka(osm_topic, osm_data)

            # Pause before the next iteration
            await asyncio.sleep(30)
        except Exception as e:
            logging.critical(f"Critical error during data processing: {e}")

# Run the asynchronous loop
if __name__ == "__main__":
    asyncio.run(main())