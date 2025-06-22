# streaming_data_app/consumer.py
# streaming_data_app/consumer.py
import json
import time
from kafka import KafkaConsumer
#import psycopg2 # NEW: For PostgreSQL connection
import pyodbc
import os # To read environment variables

# Kafka configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
KAFKA_TOPIC = 'crypto_prices'

# # PostgreSQL configuration (read from environment variables set in docker-compose)
# PG_HOST = os.getenv('PG_HOST', 'postgres')
# PG_DATABASE = os.getenv('PG_DATABASE', 'crypto_db')
# PG_USER = os.getenv('PG_USER', 'admin')
# PG_PASSWORD = os.getenv('PG_PASSWORD', 'mysecretpassword') # CHANGE THIS!

# SQL Server configuration for LOCAL machine
# 'host.docker.internal' is a special hostname used by Docker Desktop to refer to the host machine.
SQL_SERVER = os.getenv('SQL_SERVER', 'host.docker.internal') # Connect to your local Windows machine
SQL_PORT = os.getenv('SQL_PORT', '1433') # Default SQL Server port
SQL_DATABASE = os.getenv('SQL_DATABASE', 'Kafka') # Or your specific database name
SQL_USERNAME = os.getenv('SQL_USERNAME', 'Kafka') # Your SQL Server username
SQL_PASSWORD = os.getenv('SQL_PASSWORD', 'Kafka') # CHANGE THIS to your local SA password!


# --- ADD THIS DELAY ---
print("Waiting 15 seconds for Kafka to be ready for the Consumer...")
time.sleep(45) # Give Kafka some time to fully start

# Establish Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-price-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# SQL Server connection string
conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={SQL_SERVER},{SQL_PORT};" # Use host.docker.internal and port
    f"DATABASE={SQL_DATABASE};"
    f"UID={SQL_USERNAME};"
    f"PWD={SQL_PASSWORD}"
)

# Function to ensure table exists in PostgreSQL
# def ensure_table_exists(cursor):
#     cursor.execute("""
#         CREATE TABLE IF NOT EXISTS crypto_prices (
#             id SERIAL PRIMARY KEY,
#             timestamp_utc TIMESTAMP WITH TIME ZONE,
#             bitcoin_usd_price DECIMAL(18, 8),
#             ingestion_time TIMESTAMP WITH TIME ZONE DEFAULT NOW()
#         );
#     """)
#     print("Ensured 'crypto_prices' table exists.")

# Function to ensure table exists
def ensure_table_exists(cursor):
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='crypto_prices' and xtype='U')
        CREATE TABLE crypto_prices (
            id INT IDENTITY(1,1) PRIMARY KEY,
            timestamp DATETIME,
            bitcoin_usd_price DECIMAL(18, 8),
            ingestion_time DATETIME DEFAULT GETDATE()
        )
    """)
    cursor.commit()
    print("Ensured 'crypto_prices' table exists.")

print(f"Starting Kafka Consumer for topic: {KAFKA_TOPIC}")

conn = None
cursor = None # Initialize cursor outside the loop

try:
    # Loop until PostgreSQL is available
    while conn is None:
        try:
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            ensure_table_exists(cursor)
            print("Successfully connected to SQL Server and ensured table.")
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            print(f"SQL Server not ready or connection error: {sqlstate}. Retrying in 5 seconds...")
            time.sleep(5)

    for message in consumer:
        data = message.value
        timestamp_unix = data['timestamp'] # This is the Unix timestamp
        price = data['bitcoin_usd_price']

        # Convert Unix timestamp to a PostgreSQL-friendly datetime object
        # Note: PostgreSQL's TIMESTAMP WITH TIME ZONE is good practice for timestamps
        import datetime
        dt_object = datetime.datetime.fromtimestamp(timestamp_unix, tz=datetime.timezone.utc)
        sql_timestamp = dt_object.strftime('%Y-%m-%d %H:%M:%S')


        print(f"Consuming message: {data}")

        try:
            cursor.execute(
                "INSERT INTO crypto_prices (timestamp, bitcoin_usd_price) VALUES (?, ?)",
                sql_timestamp, price
            )
            conn.commit()
            print("Data inserted into SQL Server.")
        except pyodbc.Error as ex:
            print(f"Error inserting data: {ex}")
            conn.rollback()

except KeyboardInterrupt:
    print("Consumer stopped.")
except Exception as e:
    print(f"An unexpected error occurred during message consumption: {e}")
finally:
    if conn:
        conn.close()
    consumer.close()