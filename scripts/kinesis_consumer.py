import boto3
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

AWS_REGION = os.getenv("AWS_REGION")
KINESIS_STREAM_FINANCE = os.getenv("KINESIS_STREAM_FINANCE")

kinesis_client = boto3.client("kinesis", region_name=AWS_REGION)

def consume_from_kinesis(stream_name):
    """Consume data from Kinesis Stream"""
    response = kinesis_client.describe_stream(StreamName=stream_name)
    shard_id = response["StreamDescription"]["Shards"][0]["ShardId"]

    shard_iterator_response = kinesis_client.get_shard_iterator(
        StreamName=stream_name,
        ShardId=shard_id,
        ShardIteratorType="TRIM_HORIZON"
    )
    shard_iterator = shard_iterator_response["ShardIterator"]

    while True:
        records_response = kinesis_client.get_records(ShardIterator=shard_iterator, Limit=10)
        for record in records_response["Records"]:
            data = json.loads(record["Data"])
            print(f"Consumed from Kinesis: {data}")
        shard_iterator = records_response["NextShardIterator"]

if __name__ == "__main__":
    consume_from_kinesis(KINESIS_STREAM_FINANCE)
    