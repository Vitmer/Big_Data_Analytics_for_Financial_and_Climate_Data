import boto3
import json
import os
import time
import asyncio
from dotenv import load_dotenv
from threading import Thread

# Load environment variables
load_dotenv()

AWS_REGION = os.getenv("AWS_REGION")
KINESIS_STREAM_FINANCE = os.getenv("KINESIS_STREAM_FINANCE")
KINESIS_STREAM_WEATHER = os.getenv("KINESIS_STREAM_WEATHER")
KINESIS_STREAM_OSM = os.getenv("KINESIS_STREAM_OSM")

kinesis_client = boto3.client("kinesis", region_name=AWS_REGION)

def consume_from_kinesis(stream_name):
    """Consume data from Kinesis Stream"""
    try:
        response = kinesis_client.describe_stream(StreamName=stream_name)
        shard_id = response["StreamDescription"]["Shards"][0]["ShardId"]

        shard_iterator_response = kinesis_client.get_shard_iterator(
            StreamName=stream_name,
            ShardId=shard_id,
            ShardIteratorType="TRIM_HORIZON"
        )
        shard_iterator = shard_iterator_response["ShardIterator"]

        while True:
            records_response = kinesis_client.get_records(ShardIterator=shard_iterator, Limit=100)  # Increased Limit
            if "Records" in records_response and records_response["Records"]:
                for record in records_response["Records"]:
                    data = json.loads(record["Data"])
                    print(f"Consumed from Kinesis ({stream_name}): {data}")
            else:
                print(f"No new records in {stream_name}, waiting...")
            
            shard_iterator = records_response["NextShardIterator"]
            time.sleep(1)  # Wait before the next request

    except Exception as e:
        print(f"Error consuming data from Kinesis stream {stream_name}: {e}")

# Function to start consumption from a stream in a separate thread
def start_consumer_thread(stream_name):
    thread = Thread(target=consume_from_kinesis, args=(stream_name,))
    thread.daemon = True  # Daemonize thread to close it when main program exits
    thread.start()

if __name__ == "__main__":
    print(f"AWS_REGION: {AWS_REGION}")
    print(f"KINESIS_STREAM_FINANCE: {KINESIS_STREAM_FINANCE}")
    print(f"KINESIS_STREAM_WEATHER: {KINESIS_STREAM_WEATHER}")
    print(f"KINESIS_STREAM_OSM: {KINESIS_STREAM_OSM}")
    
    # Start consumer for all streams
    start_consumer_thread(KINESIS_STREAM_FINANCE)
    start_consumer_thread(KINESIS_STREAM_WEATHER)
    start_consumer_thread(KINESIS_STREAM_OSM)

    # Keep the main program running
    while True:
        time.sleep(1)