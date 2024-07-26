from kafka import KafkaConsumer
import json
import csv

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'KATL',  # Topic name (replace with your airport's ICAO code)
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',  # Start reading from the beginning of the topic
    enable_auto_commit=True,
    group_id='consumer-group-1',  # Consumer group ID
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Define the CSV file path
csv_file_path = 'received_states.csv'

# Start consuming messages
print("Waiting for messages...")

# Initialize CSV writer variables
csv_file = None
writer = None

try:
    for message in consumer:
        # Print received message
        print(f"Received message: {message.value}")

        # Open the CSV file for writing once we receive the first message
        if csv_file is None:
            csv_file = open(csv_file_path, mode='w', newline='')
            # Extract column headers from the first message
            csv_columns = list(message.value.keys())
            writer = csv.DictWriter(csv_file, fieldnames=csv_columns)
            writer.writeheader()  # Write CSV header

        # Write the received state vector to the CSV file
        writer.writerow(message.value)
        csv_file.flush()  # Ensure data is written to the file

finally:
    if csv_file:
        csv_file.close()
    consumer.close()  # Close the Kafka consumer gracefully

