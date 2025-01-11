import yfinance as yf
import osmnx as ox
from confluent_kafka import Producer
import json
import time
import logging

# Configure logging
logging.basicConfig(
    filename='logs/producer.log', 
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger().addHandler(console)

# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'data-producer'
}
producer = Producer(kafka_config)

def send_to_kafka(topic, message):
    """Send a message to Kafka"""
    try:
        producer.produce(topic, value=json.dumps(message))
        producer.flush()
        logging.info(f"Sent to Kafka: {message}")
    except Exception as e:
        logging.error(f"Failed to send message to Kafka: {e}")

# Fetch data from Yahoo Finance
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

# Fetch sample data from OpenWeatherMap
def fetch_openweather_data():
    try:
        # Example static data without an API key (OpenWeatherMap sandbox data)
        data = {
            "source": "OpenWeatherMap",
            "city": "London",
            "temperature": 280.32,  # Static example temperature in Kelvin
            "humidity": 81,        # Static example humidity percentage
            "weather": "Clear"     # Static example weather description
        }
        logging.info(f"Fetched OpenWeatherMap data: {data}")
        return data
    except Exception as e:
        logging.error(f"Error fetching OpenWeatherMap data: {e}")
        return None

# Fetch data from OpenStreetMap
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
if __name__ == "__main__":
    # Kafka topics
    finance_topic = "finance-data"
    weather_topic = "weather-data"
    osm_topic = "osm-data"

    while True:
        try:
            # Fetch Yahoo Finance data
            finance_data = fetch_yahoo_finance_data("AAPL")
            if finance_data:
                send_to_kafka(finance_topic, finance_data)

            # Fetch OpenWeatherMap data
            weather_data = fetch_openweather_data()
            if weather_data:
                send_to_kafka(weather_topic, weather_data)

            # Fetch OpenStreetMap data
            osm_data = fetch_osm_data("New York, USA")
            if osm_data:
                send_to_kafka(osm_topic, osm_data)

            # Pause before the next iteration
            time.sleep(30)
        except Exception as e:
            logging.critical(f"Critical error during data processing: {e}")