from kafka import KafkaProducer

# Initialize producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: v.encode('utf-8')
)

# Read X12 file
with open('../sample_data/sample_837.txt', 'r') as file:
    x12_data = file.read()

# Send to Kafka topic
producer.send('healthcare-claims', x12_data)

# Flush to ensure delivery
producer.flush()

print("X12 data sent to Kafka topic 'healthcare-claims'")