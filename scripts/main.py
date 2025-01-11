from stream_handler import StreamHandler
import os

# Example configuration for Kafka
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'data-consumer-group',
    'auto.offset.reset': 'earliest'
}

# Example configuration for Kinesis
kinesis_config = {
    'region': 'eu-central-1'
}

# Initialize handlers
kafka_handler = StreamHandler(system="kafka", config=kafka_config)
kinesis_handler = StreamHandler(system="kinesis", config=kinesis_config)

# Example data
data = {
    "source": "Test Source",
    "value": 123
}

# Send data to both Kafka and Kinesis
kafka_handler.send("test-topic", data)
kinesis_handler.send("test-stream", data)

# Consume data from both Kafka and Kinesis
print("Consuming from Kafka:")
for message in kafka_handler.consume("test-topic"):
    print(message)

print("Consuming from Kinesis:")
for message in kinesis_handler.consume("test-stream"):
    print(message)