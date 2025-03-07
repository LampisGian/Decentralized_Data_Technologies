import logging
import json
import requests
import time
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
BOOTSTRAP_SERVERS = ['172.25.0.13:9092'] # kafka ip:port
STATION_STATUS_TOPIC = "station_status" # kafka topic name

STATION_STATUS_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_status.json" # api for station status 

admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS) # initialization of the producer
producer = KafkaProducer( 
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def create_topics(): # topic creation 
    existing_topics = admin_client.list_topics()
    topics_to_create = []

    if STATION_STATUS_TOPIC not in existing_topics:
        topics_to_create.append(NewTopic(name=STATION_STATUS_TOPIC, num_partitions=1, replication_factor=1))

    if topics_to_create:
        admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
        logging.info("Created topics: {}".format([topic.name for topic in topics_to_create]))
    else:
        logging.info("Topics already exist.")

def fetch_station_status(): # api call
    try:
        response = requests.get(STATION_STATUS_URL)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error("Error fetching data from {}: {}".format(STATION_STATUS_URL, e))
        return None

def send_station_status(): # data send to kafka topic
    station_status = fetch_station_status()
    if station_status:
        try:
            stations_data = station_status.get("data", {}).get("stations", [])
            for station in stations_data:
                producer.send(STATION_STATUS_TOPIC, value=station)
            producer.flush()
            logging.info("Sent station status data to Kafka.")
        except Exception as e:
            logging.error("Failed to send station status to Kafka: {}".format(e))

if __name__ == "__main__":
    logging.info("Starting Station Status Producer...")
    create_topics()

    try:
        while True:
            send_station_status()
            logging.info("Data sent successfully! Waiting for the next cycle...")
            time.sleep(2)
    except KeyboardInterrupt:
        logging.info("Shutting down producer...")
        producer.close()  
