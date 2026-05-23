# ============================================================
# Notebook 03: Azure Event Hub Producer
# ============================================================
# Sends simulated weather data to Azure Event Hub.
# Prerequisites: pip install azure-eventhub
# ============================================================

# CELL 1: Install library
# %pip install azure-eventhub

# CELL 2: Producer Code
import json
import time
import random
from datetime import datetime
from azure.eventhub import EventHubProducerClient, EventData, TransportType

# ==============================
# CONNECTION DETAILS - Replace with YOUR values
# ==============================
CONNECTION_STR = "YOUR_CONNECTION_STRING_HERE"
EVENTHUB_NAME = "weather-events"

# ==============================
# Use WebSocket transport (port 443) - fixes CBS auth errors in Fabric
# ==============================
producer = EventHubProducerClient.from_connection_string(
    conn_str=CONNECTION_STR,
    eventhub_name=EVENTHUB_NAME,
    transport_type=TransportType.AmqpOverWebsocket
)

cities = ["Bengaluru", "Mumbai", "Delhi", "Chennai", "Hyderabad"]

print("Sending events to Event Hub...")

try:
    for i in range(100):
        event_data = {
            "city": random.choice(cities),
            "temperature": round(random.uniform(15, 45), 2),
            "humidity": random.randint(30, 95),
            "wind_speed": round(random.uniform(0, 30), 2),
            "timestamp": datetime.utcnow().isoformat()
        }

        event_batch = producer.create_batch()
        event_batch.add(EventData(json.dumps(event_data)))
        producer.send_batch(event_batch)

        print(f"Sent event {i+1}: {event_data['city']} | {event_data['temperature']}C")
        time.sleep(1)

except Exception as e:
    print(f"Error: {e}")

finally:
    producer.close()
    print("Producer closed!")
