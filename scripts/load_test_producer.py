from confluent_kafka import Producer
import json
import random
import time

# Kafka configuration
producer_config = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(producer_config)

# Топик для тестирования
topic = "finance-data"

# Генерация данных
def generate_finance_data():
    return {
        "source": "Test Data Generator",
        "ticker": "TEST",
        "date": time.strftime("%Y-%m-%d %H:%M:%S"),
        "open": random.uniform(200, 300),
        "close": random.uniform(200, 300),
        "high": random.uniform(200, 300),
        "low": random.uniform(200, 300),
        "volume": random.randint(1000, 10000)
    }

# Отправка данных в Kafka
try:
    for _ in range(1000):  # Отправляем 1000 сообщений
        message = generate_finance_data()
        producer.produce(topic, value=json.dumps(message))
        print(f"Sent: {message}")
        producer.flush()
        time.sleep(0.01)  # 10 миллисекунд паузы
except KeyboardInterrupt:
    print("Load test stopped.")
    