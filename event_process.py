from confluent_kafka import Consumer, Producer, KafkaError
import json
import time
from collections import defaultdict
import os

# Kafka Consumer Configuration using environment variables
consumer_conf = {
    'bootstrap.servers': os.getenv('BOOTSTRAP_SERVERS', 'localhost:29092'),
    'group.id': os.getenv('GROUP_ID', 'my-consumer-group'),
    'auto.offset.reset': os.getenv('AUTO_OFFSET_RESET', 'earliest')
}

# Kafka Producer Configuration using environment variables
producer_conf = {
    'bootstrap.servers': os.getenv('BOOTSTRAP_SERVERS', 'localhost:29092')
}

consumer = Consumer(consumer_conf)
producer = Producer(producer_conf)

# Subscribe to the input topic specified by an environment variable
input_topic = os.getenv('INPUT_TOPIC', 'user-login')
consumer.subscribe([input_topic])

# Output topic specified by an environment variable
output_topic = os.getenv('OUTPUT_TOPIC', 'processed-user-login')

# Aggregation dictionaries
device_type_count = defaultdict(int)
locale_count = defaultdict(int)
date_count = defaultdict(int)

def process_message(message):
    data = json.loads(message.value().decode('utf-8'))

    # Filter: Only process logins from the specified app version
    target_app_version = os.getenv('TARGET_APP_VERSION', '2.3.0')
    if data['app_version'] != target_app_version:
        return None

    # Add a processed timestamp
    data['processed_timestamp'] = int(time.time())

    # Aggregation: Count logins by device type and locale
    data['device_type'] = data.get('device_type', 'Unknown')
    data['locale'] = data.get('locale', 'Unknown')
    device_type_count[data['device_type']] = device_type_count.get(data['device_type'], 0) + 1
    locale_count[data['locale']] = locale_count.get(data['locale'], 0) + 1
    # Date count aggregation by day, precision to the minute
    login_date = time.strftime('%Y-%m-%d %H:%M', time.gmtime(data['timestamp']))
    date_count[login_date] = date_count.get(login_date, 0) + 1

    # Add the aggregates to the data
    data['device_type_count'] = device_type_count
    data['locale_count'] = locale_count
    data['date_count'] = date_count
    
    # Print all logins by date, erasing the previous output
    os.system('clear')
    print("Logins by Date:")
    for date, count in date_count.items():
        print(f"{date}: {count}")
    return data

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        processed_data = process_message(msg)
        if processed_data:
            producer.produce(output_topic, json.dumps(processed_data).encode('utf-8'))
            producer.flush()

except KeyboardInterrupt:
    pass
finally:
    consumer.close()

# Print aggregated results (for demonstration purposes)
print("Logins by Device Type:")
for device_type, count in device_type_count.items():
    print(f"{device_type}: {count}")

print("\nLogins by Locale:")
for locale, count in locale_count.items():
    print(f"{locale}: {count}")
