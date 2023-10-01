from kafka import KafkaConsumer
import json
import psycopg2

# Set up the Kafka Consumer
consumer = KafkaConsumer(
    'ecommerce_activity',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',  # start at the beginning of the topic
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Establish connection to PostgreSQL
conn = psycopg2.connect(
    dbname='ecommerce_db',
    user='postgres',
    password='1234',
    host='localhost',
    port='5432'
)
cursor = conn.cursor()

try:
    for message in consumer:
        # Parse message
        data = message.value
        
        # Insert data into PostgreSQL
        query = """
        INSERT INTO activity_log (user_id, activity_type, product_id, timestamp)
        VALUES (%s, %s, %s, %s);
        """
        cursor.execute(query, (data['user_id'], data['activity_type'], data['product_id'], data['timestamp']))
        
        # Commit changes
        conn.commit()

        print(f"Stored: {data}")

except KeyboardInterrupt:
    pass

finally:
    cursor.close()
    conn.close()