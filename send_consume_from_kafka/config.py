# Define the Kafka configuration
kafka_conf = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka server
    'client.id': 'python-producer',
}

# Define the Kafka topic to send data to
producer_topic = 'weather_topic'  # Replace with your Kafka topic

# Define the Kafka topic
topic = 'registered_user'  # Replace with your Kafka topic

# Define the Kafka consumer configuration
kafka_consumer_conf = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka server
    'group.id': 'python-consumer',
    'auto.offset.reset': 'earliest',  # Start consuming from the beginning
}