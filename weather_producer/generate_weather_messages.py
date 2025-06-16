import requests
import json
import os
import time

from dotenv import load_dotenv
from azure.eventhub import EventHubProducerClient, EventData


load_dotenv()

# set to track The Hague
LAT = 52.076066
LON =  4.317899

# connect to hub
APIKEY = os.getenv("APIKEY")
CONN_STR = os.getenv("CONN_STR")
EVENTHUB = os.getenv("EVENTHUB")

def create_data_message():
    response = requests.get(f"https://api.openweathermap.org/data/2.5/weather?lat={LAT}&lon={LON}&appid={APIKEY}")
    data = json.loads(response.content)

    return data

def send_data(data: dict):
    # create producer
    producer = EventHubProducerClient.from_connection_string(conn_str=CONN_STR, eventhub_name=EVENTHUB)

    try:
        batch = producer.create_batch()
        data = create_data_message()
        batch.add(EventData(json.dumps(data)))
        producer.send_batch(batch)
        print("message send succesfully")
    except Exception as e:
        print(f"failed: {e}")
        raise e
    finally:
        print("closing producer")
        producer.close()


if __name__ == "__main__":
    print(CONN_STR)
    # send a message every 5 seconds
    for i in range(1):
        data = create_data_message()
        send_data(data)
        time.sleep(3)