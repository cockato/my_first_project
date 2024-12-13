from confluent_kafka import Producer, Consumer, KafkaException
import config
import json
import requests


def send_message_to_kafka():
    response = requests.get("https://api.open-meteo.com/v1/forecast",
                            params={
                                "latitude": 51.5,
                                "longitude": -0.11,
                                "current": "temperature_2m"
                            }
                            )
    print(response.json())

    weather = response.json()

    # Create a producer instance
    producer = Producer(config.kafka_conf)

    # Callback to handle delivery report (optional)
    def delivery_report(err, msg):
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

    # Produce a message to the Kafka topic
    try:
        # Send the data as a JSON string
        producer.produce(config.producer_topic, key="key-1", value=json.dumps(weather), callback=delivery_report)
        # Wait for the message to be delivered
        producer.flush()
    except Exception as e:
        print(f"Error producing message: {e}")


def consume_message_from_kafka():
    # Create a consumer instance
    consumer = Consumer(config.kafka_consumer_conf)

    # Subscribe to the topic
    consumer.subscribe([config.topic])

    # Consume messages
    try:
        while True:
            msg = consumer.poll(timeout=1.0)  # Poll for a message for 1 second

            if msg is None:  # No new message
                continue
            if msg.error():  # Error handling
                if msg.error() is not None:
                    print(f"End of partition reached {msg.partition} {msg.offset}")
                else:
                    raise KafkaException(msg.error())
            else:
                print(f"Consumed message: {msg.value().decode('utf-8')}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    send_message_to_kafka()
    # consume_message_from_kafka()

