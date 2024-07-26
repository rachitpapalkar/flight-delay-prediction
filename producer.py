from kafka import KafkaProducer
import json

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send a test message
producer.send('test-topic', {'key': 'value'})
producer.send('test-topic', {'message': 'This is a test message'})

# Flush and close the producer
producer.flush()
producer.close()
print("Messages sent successfully")

