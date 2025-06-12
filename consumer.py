# streaming_data_app/consumer.py
import json
import time
from kafka import KafkaConsumer
import pyodbc # For SQL Server connection

# Kafka configuration
KAFKA_BROKER = 'kafka:9092'
KAFKA_TOPIC = 'crypto_prices'

# SQL Server configuration
SQL_SERVER = 'sqlserver' # 'sqlserver' is the service name in docker-compose
SQL_DATABASE = 'master' # Or create a dedicated DB if you want
SQL_USERNAME = 'sa'
SQL_PASSWORD = 'Rod' # CHANGE THIS - MUST MATCH docker-compose.yml

# Establish Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest', # Start reading from the beginning if no offset is committed
    enable_auto_commit=True,
    group_id='my-price-group', # Consumer group ID
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# SQL Server connection string
# For SQL Server on Linux, you typically use the ODBC Driver 17 or 18 for SQL Server
# You'll need to install this driver in your Dockerfile for the consumer.
conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};" # Or 18 for SQL Server, if you install it
    f"SERVER={SQL_SERVER};"
    f"DATABASE={SQL_DATABASE};"
    f"UID={SQL_USERNAME};"
    f"PWD={SQL_PASSWORD}"
)

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
try:
    # Loop until SQL Server is available
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
        timestamp = data['timestamp']
        price = data['bitcoin_usd_price']

        # Convert Unix timestamp to a format SQL Server likes (datetime)
        # You might need to adjust this depending on your SQL Server version/column type
        # Using datetime.fromtimestamp if you prefer precise Python datetime objects, then format
        import datetime
        dt_object = datetime.datetime.fromtimestamp(timestamp)
        sql_timestamp = dt_object.strftime('%Y-%m-%d %H:%M:%S') # Format for SQL Server DATETIME

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
            conn.rollback() # Rollback on error

except KeyboardInterrupt:
    print("Consumer stopped.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
finally:
    if conn:
        conn.close()
    consumer.close()