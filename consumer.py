# streaming_data_app/consumer.py
# streaming_data_app/consumer.py
import json
import time
from kafka import KafkaConsumer
import psycopg2 # NEW: For PostgreSQL connection
import os # To read environment variables

# Kafka configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
KAFKA_TOPIC = 'crypto_prices'

# PostgreSQL configuration (read from environment variables set in docker-compose)
PG_HOST = os.getenv('PG_HOST', 'postgres')
PG_DATABASE = os.getenv('PG_DATABASE', 'crypto_db')
PG_USER = os.getenv('PG_USER', 'admin')
PG_PASSWORD = os.getenv('PG_PASSWORD', 'mysecretpassword') # CHANGE THIS!

# Establish Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-price-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Function to ensure table exists in PostgreSQL
def ensure_table_exists(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS crypto_prices (
            id SERIAL PRIMARY KEY,
            timestamp_utc TIMESTAMP WITH TIME ZONE,
            bitcoin_usd_price DECIMAL(18, 8),
            ingestion_time TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
    """)
    print("Ensured 'crypto_prices' table exists.")

print(f"Starting Kafka Consumer for topic: {KAFKA_TOPIC}")

conn = None
cursor = None # Initialize cursor outside the loop

try:
    # Loop until PostgreSQL is available
    while conn is None:
        try:
            conn = psycopg2.connect(
                host=PG_HOST,
                database=PG_DATABASE,
                user=PG_USER,
                password=PG_PASSWORD
            )
            conn.autocommit = True # Auto-commit changes for simplicity in this script
            cursor = conn.cursor()
            ensure_table_exists(cursor)
            print("Successfully connected to PostgreSQL and ensured table.")
        except psycopg2.OperationalError as ex:
            print(f"PostgreSQL not ready or connection error: {ex}. Retrying in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            print(f"An unexpected error occurred during DB connection setup: {e}")
            time.sleep(5)

    for message in consumer:
        data = message.value
        timestamp_unix = data['timestamp'] # This is the Unix timestamp
        price = data['bitcoin_usd_price']

        # Convert Unix timestamp to a PostgreSQL-friendly datetime object
        # Note: PostgreSQL's TIMESTAMP WITH TIME ZONE is good practice for timestamps
        import datetime
        dt_object = datetime.datetime.fromtimestamp(timestamp_unix, tz=datetime.timezone.utc)


        print(f"Consuming message: {data}")

        try:
            cursor.execute(
                "INSERT INTO crypto_prices (timestamp_utc, bitcoin_usd_price) VALUES (%s, %s)",
                (dt_object, price) # psycopg2 uses %s placeholders
            )
            # No conn.commit() needed if autocommit is True
            print("Data inserted into PostgreSQL.")
        except Exception as e:
            print(f"Error inserting data: {e}")
            # If autocommit is False, you would need conn.rollback() here

except KeyboardInterrupt:
    print("Consumer stopped.")
except Exception as e:
    print(f"An unexpected error occurred during message consumption: {e}")
finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
    consumer.close()