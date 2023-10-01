from kafka import KafkaProducer
import json
import time
import random

# Setting up the Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# User activities we'd like to simulate
ACTIVITIES = ["page_view", "product_view", "cart_addition", "purchase"]

def simulate_activity():
    """
    Simulates a user activity and returns a dictionary representation of that activity.
    """
    user_id = random.randint(1, 1000)  # Simulating user IDs between 1 and 1000
    activity = random.choice(ACTIVITIES)
    product_id = random.randint(1, 100) if activity != "page_view" else None  # Simulating product IDs between 1 and 100

    return {
        "user_id": user_id,
        "activity_type": activity,
        "product_id": product_id,
        "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
    }

# Infinite loop to continuously send messages
try:
    while True:
        activity = simulate_activity()
        producer.send('ecommerce_activity', activity)
        print(f"Sent: {activity}")
        time.sleep(random.randint(1, 5))  # Sleep between 1 to 5 seconds
except KeyboardInterrupt:
    producer.close()