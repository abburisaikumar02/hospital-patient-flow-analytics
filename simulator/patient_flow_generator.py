import json
import random
import uuid
import time
from datetime import datetime, timedelta, timezone
from azure.eventhub import EventHubProducerClient, EventData

# 🔹 Azure Event Hub Configuration (replace placeholder with your actual connection string)
CONNECTION_STRING = "<<EVENT_HUB_CONNECTION_STRING>>"

# Departments in hospital
departments = ["Emergency", "Surgery", "ICU", "Pediatrics", "Maternity", "Oncology", "Cardiology"]

# Gender categories 
genders = ["Male", "Female"]

# Helper function to introduce dirty data
def inject_dirty_data(record):
    # 5% chance to have invalid age
    if random.random() < 0.05:
        record["age"] = random.randint(101, 150)

    # 5% chance to have future admission timestamp
    if random.random() < 0.05:
        record["admission_time"] = (datetime.now(timezone.utc) + timedelta(hours=random.randint(1, 72))).isoformat()

    return record

def generate_patient_event():
    admission_time = datetime.now(timezone.utc) - timedelta(hours=random.randint(0, 72))
    discharge_time = admission_time + timedelta(hours=random.randint(1, 72))

    # Randomly assign status for testing spikes
    status_roll = random.random()
    if status_roll < 0.1:
        status = "ServerError"      # 10% server errors
    elif status_roll < 0.15:
        status = "UserError"        # 5% user errors
    else:
        status = "Success"          # 85% success

    event = {
        "patient_id": str(uuid.uuid4()),
        "gender": random.choice(genders),
        "age": random.randint(1, 100),
        "department": random.choice(departments),
        "admission_time": admission_time.isoformat(),
        "discharge_time": discharge_time.isoformat(),
        "bed_id": random.randint(1, 500),
        "hospital_id": random.randint(1, 7),
        "status": status,
        "EntityName": "hospital-analytical-data"
    }

    return inject_dirty_data(event)

if __name__ == "__main__":
    if CONNECTION_STRING.startswith("<<") or CONNECTION_STRING.endswith(">>"):
        raise ValueError("⚠️ Please replace the CONNECTION_STRING placeholder with your actual Event Hub connection string")

    # Create Event Hub producer
    producer = EventHubProducerClient.from_connection_string(conn_str=CONNECTION_STRING)

    try:
        while True:
            event_data_batch = producer.create_batch()
            
            # Send multiple events per second for visible spikes
            for _ in range(10):  
                event = generate_patient_event()
                event_data_batch.add(EventData(json.dumps(event)))
                print(f"Sent to Event Hub: {event}")

            producer.send_batch(event_data_batch)
            time.sleep(1)  # 1-second interval between batches

    except KeyboardInterrupt:
        print("Stopping data generation...")

    finally:
        producer.close()
