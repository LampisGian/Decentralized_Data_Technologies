import logging
import json
import requests
import time
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
BOOTSTRAP_SERVERS = ['172.25.0.13:9092'] # kafka ip:port
STATION_INFO_TOPIC = "station_information" # kafka topic name

STATION_INFO_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_information.json" # api for station info

admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS) # initialization of the producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def create_topics(): # topic creation
    existing_topics = admin_client.list_topics()
    topics_to_create = []

    if STATION_INFO_TOPIC not in existing_topics:
        topics_to_create.append(NewTopic(name=STATION_INFO_TOPIC, num_partitions=1, replication_factor=1))

    if topics_to_create:
        admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
        logging.info("Created topics: {}".format([topic.name for topic in topics_to_create]))
    else:
        logging.info("Topics already exist.")

def fetch_station_info(): # api call
    try:
        response = requests.get(STATION_INFO_URL)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error("Error fetching data from {}: {}".format(STATION_INFO_URL, e))
        return None

def send_station_info(): # data send to kafka topic
    station_info = fetch_station_info()
    if station_info:
        try:
            stations_data = station_info.get("data", {}).get("stations", [])
            for station in stations_data:
                producer.send(STATION_INFO_TOPIC, value=station)
            producer.flush()
            logging.info("Sent station information data to Kafka.")
        except Exception as e:
            logging.error("Failed to send station information to Kafka: {}".format(e))

if __name__ == "__main__":
    logging.info("Starting Station Status Producer...")
    create_topics()

    try:
        while True:
            send_station_info()
            logging.info("Data sent successfully! Waiting for the next cycle...")
            time.sleep(2) #wait for 2 seconds before sending the next request
    except KeyboardInterrupt:
        logging.info("Shutting down producer...")
        producer.close()  
