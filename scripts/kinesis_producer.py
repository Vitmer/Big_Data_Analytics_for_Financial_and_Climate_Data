import boto3
import json
import os
import logging
import redis
import time
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler
from botocore.exceptions import ClientError

import logging

# Настройка логирования
logging.basicConfig(level=logging.DEBUG)

# Логирование начала скрипта
logging.info("Starting the Kinesis producer script.")

# Load environment variables
load_dotenv()

AWS_REGION = os.getenv("AWS_REGION")
KINESIS_STREAM_FINANCE = os.getenv("KINESIS_STREAM_FINANCE")
KINESIS_STREAM_WEATHER = os.getenv("KINESIS_STREAM_WEATHER")
KINESIS_STREAM_OSM = os.getenv("KINESIS_STREAM_OSM")

# Setup Kinesis client
kinesis_client = boto3.client("kinesis", region_name=AWS_REGION)

# Setup Redis for preventing duplicate messages
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

# Setting up log rotation
log_handler = RotatingFileHandler('logs/kinesis_producer.log', maxBytes=5*1024*1024, backupCount=3)
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

def send_to_kinesis(stream_name, data):
    """Send data to Kinesis Stream with duplication check"""
    try:
        # Create a unique key based on data
        message_key = f"{data.get('ticker', '')}_{data.get('date', '')}"
        
        # Check if the message has already been sent (using Redis)
        logging.debug(f"Checking if message is duplicate for key: {message_key}")
        if redis_client.get(message_key) is None:
            logging.debug(f"Message is unique. Sending to Kinesis stream {stream_name}.")
            # If the message is unique, send it to Kinesis
            response = kinesis_client.put_record(
                StreamName=stream_name,
                Data=json.dumps(data),
                PartitionKey="partition_key"
            )
            # Mark the message as sent in Redis
            redis_client.set(message_key, 'sent')
            logging.info(f"Sent to Kinesis: {data}, Response: {response}")
        else:
            logging.info(f"Duplicate message skipped: {data}")
    except ClientError as e:
        logging.error(f"Client error sending to Kinesis: {e}")
    except Exception as e:
        logging.error(f"Error sending to Kinesis: {e}")

# Function to handle retries with exponential backoff
def send_with_retries(stream_name, data, retries=5, delay=1):
    """Send data with retry logic"""
    for attempt in range(retries):
        try:
            logging.debug(f"Attempt {attempt + 1} of {retries}")
            send_to_kinesis(stream_name, data)
            return
        except Exception as e:
            logging.error(f"Attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(delay)
                delay *= 2  # Exponential backoff
            else:
                logging.critical("Max retries reached, failed to send data to Kinesis.")

if __name__ == "__main__":
    finance_data = {
        "source": "Yahoo Finance",
        "ticker": "MSFT",  # Измените тикер на другой
        "date": "2025-01-11",  # Или измените дату
        "open": 240.0,
        "close": 236.8,
        "high": 240.1,
        "low": 233.0,
        "volume": 58293813
    }
    logging.debug(f"Starting to send finance data: {finance_data}")
    send_with_retries(KINESIS_STREAM_FINANCE, finance_data)