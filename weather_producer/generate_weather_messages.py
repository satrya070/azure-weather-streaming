import requests
import json
import os
import time
import logging

from dotenv import load_dotenv
from azure.eventhub import EventHubProducerClient, EventData


load_dotenv()

# logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d-%H:%m:%S"
)

# set to track The Hague
LAT = 52.076066
LON =  4.317899

# connect to hub
APIKEY = os.getenv("APIKEY")
CONN_STR = os.getenv("CONN_STR")
EVENTHUB = os.getenv("EVENTHUB")

producer = EventHubProducerClient.from_connection_string(conn_str=CONN_STR, eventhub_name=EVENTHUB)

def create_data_message():
    response = requests.get(f"https://api.openweathermap.org/data/2.5/weather?lat={LAT}&lon={LON}&appid={APIKEY}")
    data = json.loads(response.content)

    return data

def send_data():
    # create producer
    data = create_data_message()

    try:
        batch = producer.create_batch()
        data = create_data_message()
        batch.add(EventData(json.dumps(data)))
        #producer.send_batch(batch)
        producer.send_event(EventData(json.dumps(data)))
        logging.info("message send succesfully")
    except Exception as e:
        logging.error(f"failed: {e}")
        raise e

def retry_with_delay(
    fn,
    max_retries=5,
    delay=30.0
):
    for attempt in range(max_retries):
        try:
            return fn()
        except Exception as e:
            logging.error(f"attempt {attempt}: function failed on error: {e}")
            time.sleep(delay)
    
    raise RuntimeError(f"functions failed after {max_retries} retries.")


if __name__ == "__main__":
    AMOUNT_MESSAGES = 28_800  # this should be around 2 days with a mesage per 6 seconds
    retry_streak = 0

    try:
        # send a message every 6 seconds
        for i in range(AMOUNT_MESSAGES):
            try:
                retry_with_delay(send_data)

                # reset retry streak if succeeded again after failed execution
                if retry_streak > 0:
                    retry_streak = 0
            except Exception as final_error:
                logging.error(f"failed after 5 retries on {final_error}")
                retry_blocks += 1

                if retry_blocks == 10:
                    raise Exception("max retry block reached, stopping execution.")
                
            time.sleep(6)
    finally:
        logging.info("done, closing producer")
        producer.close()