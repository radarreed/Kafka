# streaming_data_app/producer.py
import requests
import json
import time
from kafka import KafkaProducer

# Kafka configuration
KAFKA_BROKER = 'kafka:9092' # 'kafka' is the service name in docker-compose
KAFKA_TOPIC = 'crypto_prices'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# CoinGecko API configuration
API_URL = 'https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd'
POLL_INTERVAL_SECONDS = 60 # Fetch data every 5 seconds

print(f"Starting Kafka Producer for topic: {KAFKA_TOPIC}")

try:
    while True:
        response = requests.get(API_URL)
        data = response.json()
        if 'bitcoin' in data and 'usd' in data['bitcoin']:
            price = data['bitcoin']['usd']
            message = {'timestamp': time.time(), 'bitcoin_usd_price': price}
            print(f"Producing message: {message}")
            producer.send(KAFKA_TOPIC, value=message)
        else:
            print("Unexpected API response format.")

        time.sleep(POLL_INTERVAL_SECONDS)

except KeyboardInterrupt:
    print("Producer stopped.")
finally:
    producer.close()