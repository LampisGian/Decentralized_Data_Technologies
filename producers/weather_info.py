import requests
import json
import logging
import time
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
BOOTSTRAP_SERVERS = ['172.25.0.13:9092'] # kafka ip:port
WEATHER_TOPIC = "weather_info" # kafka topic name

producer = KafkaProducer( # kafka producer initiliazition
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast" # api for weather info
NEW_YORK_COORDINATES = {"latitude": 40.7128, "longitude": -74.0060} # coordinates of NY


def fetch_weather_data():  # API call
    params = {
        "latitude": NEW_YORK_COORDINATES["latitude"],
        "longitude": NEW_YORK_COORDINATES["longitude"],
        "current_weather": True,
        "timezone": "auto",
        "hourly": "precipitation,cloudcover"  
    }
    try:
        response = requests.get(OPEN_METEO_URL, params=params, timeout=10)
        response.raise_for_status()
        weather_data = response.json()
        if "current_weather" in weather_data:  
            current_weather = weather_data["current_weather"]
            hourly_data = weather_data.get("hourly", {}) # hourly data contains cloudiness
            precipitation = hourly_data.get("precipitation", [0])[0] if "precipitation" in hourly_data else 0
            cloudiness = hourly_data.get("cloudcover", [None])[0] if "cloudcover" in hourly_data else None
            return {
                "location_name": "New York",
                "latitude": params["latitude"],
                "longitude": params["longitude"],
                "temperature": current_weather.get("temperature"),
                "wind_speed": current_weather.get("windspeed"),
                "precipitation": precipitation, 
                "cloudiness": cloudiness,   
                "timestamp": current_weather.get("time"),
            }
        else:
            logging.warning("No current weather data found in API response.")
            return None
    except requests.RequestException as e:
        logging.error("Error fetching weather data from Open-Meteo: {}".format(e))
        return None


def send_weather_data_for_ny(): # send data to kafka
    weather_data = fetch_weather_data()
    if weather_data:
        try:
            producer.send(WEATHER_TOPIC, value=weather_data)
            logging.info("Sent weather data for New York to Kafka.")
        except Exception as e:
            logging.error("Failed to send weather data to Kafka: {}".format(e))
    else:
        logging.warning("No weather data available for New York.")


if __name__ == "__main__":
    logging.info("Starting Weather Data Producer for New York...")
    try:
        while True:
            send_weather_data_for_ny()
            producer.flush()
            logging.info("Weather data sent successfully! Waiting for the next cycle...")
            time.sleep(10)
    except KeyboardInterrupt:
        logging.info("Shutting down producer...")
        producer.close()  
