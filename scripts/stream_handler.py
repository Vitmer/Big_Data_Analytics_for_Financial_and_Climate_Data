import boto3
from confluent_kafka import Producer, Consumer, KafkaException
import json
import os

class StreamHandler:
    def __init__(self, system, config):
        """
        Initialize the stream handler for Kafka or Kinesis.
        
        :param system: 'kafka' or 'kinesis'
        :param config: Dictionary containing configuration for the selected system
        """
        self.system = system

        if system == "kafka":
            self.producer = Producer({'bootstrap.servers': config['bootstrap.servers']})
            self.consumer = Consumer({
                'bootstrap.servers': config['bootstrap.servers'],
                'group.id': config['group.id'],
                'auto.offset.reset': config['auto.offset.reset']
            })
        elif system == "kinesis":
            self.client = boto3.client(
                "kinesis",
                region_name=config["region"]
            )
        else:
            raise ValueError("Unsupported system. Use 'kafka' or 'kinesis'.")

    def send(self, topic_or_stream, data):
        """
        Send data to Kafka or Kinesis.
        
        :param topic_or_stream: Kafka topic or Kinesis stream name
        :param data: Data to send (as a dictionary)
        """
        if self.system == "kafka":
            self.producer.produce(topic_or_stream, value=json.dumps(data))
            self.producer.flush()
            print(f"Sent to Kafka topic {topic_or_stream}: {data}")
        elif self.system == "kinesis":
            self.client.put_record(
                StreamName=topic_or_stream,
                Data=json.dumps(data),
                PartitionKey="partition_key"
            )
            print(f"Sent to Kinesis stream {topic_or_stream}: {data}")

    def consume(self, topic_or_stream):
        """
        Consume data from Kafka or Kinesis.
        
        :param topic_or_stream: Kafka topic or Kinesis stream name
        """
        if self.system == "kafka":
            self.consumer.subscribe([topic_or_stream])
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Kafka error: {msg.error()}")
                    continue
                yield json.loads(msg.value().decode('utf-8'))
        elif self.system == "kinesis":
            response = self.client.describe_stream(StreamName=topic_or_stream)
            shard_id = response["StreamDescription"]["Shards"][0]["ShardId"]

            shard_iterator_response = self.client.get_shard_iterator(
                StreamName=topic_or_stream,
                ShardId=shard_id,
                ShardIteratorType="TRIM_HORIZON"
            )
            shard_iterator = shard_iterator_response["ShardIterator"]

            while True:
                records_response = self.client.get_records(ShardIterator=shard_iterator, Limit=10)
                for record in records_response["Records"]:
                    yield json.loads(record["Data"])
                shard_iterator = records_response["NextShardIterator"]

    def close(self):
        """Close the Kafka consumer if used."""
        if self.system == "kafka":
            self.consumer.close()