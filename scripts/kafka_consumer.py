from confluent_kafka import Consumer, KafkaException
import json
import sqlite3
import boto3
from dotenv import load_dotenv
import os
import logging

# Ensure logs directory exists
os.makedirs('logs', exist_ok=True)

# Configure logging
logging.basicConfig(
    filename='logs/consumer.log',
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
    'group.id': 'data-consumer-group',
    'auto.offset.reset': 'earliest'
}

try:
    consumer = Consumer(kafka_config)
except Exception as e:
    logging.critical(f"Failed to initialize Kafka consumer: {e}")
    raise

# SQLite configuration
try:
    db_connection = sqlite3.connect("data.db")
    db_cursor = db_connection.cursor()
except Exception as e:
    logging.critical(f"Failed to connect to SQLite database: {e}")
    raise

# AWS S3 configuration
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

try:
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
except Exception as e:
    logging.critical(f"Failed to initialize AWS S3 client: {e}")
    raise

# Create tables if they don't exist
try:
    db_cursor.execute("""
    CREATE TABLE IF NOT EXISTS finance_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT,
        ticker TEXT,
        date TEXT,
        open REAL,
        close REAL,
        high REAL,
        low REAL,
        volume INTEGER
    )
    """)
    db_cursor.execute("""
    CREATE TABLE IF NOT EXISTS weather_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT,
        city TEXT,
        temperature REAL,
        humidity INTEGER,
        weather TEXT
    )
    """)
    db_cursor.execute("""
    CREATE TABLE IF NOT EXISTS osm_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT,
        place TEXT,
        edges_count INTEGER,
        average_length REAL
    )
    """)
    db_connection.commit()
except Exception as e:
    logging.critical(f"Failed to initialize SQLite tables: {e}")
    raise

# Save data to SQLite
def save_to_sqlite(data, table):
    try:
        keys = ", ".join(data.keys())
        placeholders = ", ".join("?" * len(data))
        values = tuple(data.values())
        db_cursor.execute(f"INSERT INTO {table} ({keys}) VALUES ({placeholders})", values)
        db_connection.commit()
        logging.info(f"Saved data to SQLite table {table}: {data}")
    except Exception as e:
        logging.error(f"Failed to save data to SQLite table {table}: {e}")

# Upload data to S3
def upload_to_s3(data, bucket, key):
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(data)
        )
        logging.info(f"Uploaded data to S3: {key}")
    except Exception as e:
        logging.error(f"Failed to upload data to S3 bucket {bucket}: {e}")

# Process finance data
def process_finance_data(message):
    try:
        data = json.loads(message)
        save_to_sqlite(data, "finance_data")
        upload_to_s3(data, S3_BUCKET_NAME, f"finance_data/{data['date']}_{data['ticker']}.json")
        logging.info(f"Processed Finance Data: {data}")
    except Exception as e:
        logging.error(f"Error processing finance data: {e}")

# Process weather data
def process_weather_data(message):
    try:
        data = json.loads(message)
        save_to_sqlite(data, "weather_data")
        upload_to_s3(data, S3_BUCKET_NAME, f"weather_data/{data['city']}.json")
        logging.info(f"Processed Weather Data: {data}")
    except Exception as e:
        logging.error(f"Error processing weather data: {e}")

# Process OSM data
def process_osm_data(message):
    try:
        data = json.loads(message)
        save_to_sqlite(data, "osm_data")
        upload_to_s3(data, S3_BUCKET_NAME, f"osm_data/{data['place']}.json")
        logging.info(f"Processed OSM Data: {data}")
    except Exception as e:
        logging.error(f"Error processing OSM data: {e}")

# Main process
if __name__ == "__main__":
    topics = ["finance-data", "weather-data", "osm-data"]
    try:
        consumer.subscribe(topics)
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logging.error(f"Kafka error: {msg.error()}")
                continue

            topic = msg.topic()
            message = msg.value().decode('utf-8')
            if topic == "finance-data":
                process_finance_data(message)
            elif topic == "weather-data":
                process_weather_data(message)
            elif topic == "osm-data":
                process_osm_data(message)
    except KeyboardInterrupt:
        logging.info("Consumer stopped by user.")
    except Exception as e:
        logging.critical(f"Unexpected error: {e}")
    finally:
        consumer.close()
        db_connection.close()
        logging.info("Consumer connection closed.")