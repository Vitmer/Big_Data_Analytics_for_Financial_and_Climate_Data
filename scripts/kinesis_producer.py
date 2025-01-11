import boto3
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

AWS_REGION = os.getenv("AWS_REGION")
KINESIS_STREAM_FINANCE = os.getenv("KINESIS_STREAM_FINANCE")
KINESIS_STREAM_WEATHER = os.getenv("KINESIS_STREAM_WEATHER")
KINESIS_STREAM_OSM = os.getenv("KINESIS_STREAM_OSM")

kinesis_client = boto3.client("kinesis", region_name=AWS_REGION)

def send_to_kinesis(stream_name, data):
    """Send data to Kinesis Stream"""
    try:
        kinesis_client.put_record(
            StreamName=stream_name,
            Data=json.dumps(data),
            PartitionKey="partition_key"
        )
        print(f"Sent to Kinesis: {data}")
    except Exception as e:
        print(f"Error sending to Kinesis: {e}")

if __name__ == "__main__":
    finance_data = {
        "source": "Yahoo Finance",
        "ticker": "AAPL",
        "date": "2025-01-10",
        "open": 240.0,
        "close": 236.8,
        "high": 240.1,
        "low": 233.0,
        "volume": 58293813
    }
    send_to_kinesis(KINESIS_STREAM_FINANCE, finance_data)
    