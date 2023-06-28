import time
import requests
from datetime import datetime
from confluent_kafka import Producer
import json
import logging



city_api_key = "d7Qo4kY4Mf+OOgehs8qEYw==b3xQE8f6NRdmR2DV"
meteo_api_key = "c595574d1e5647d87599ff402148b77a"
european_capitals = [
    "Andorra la Vella", "Tirana", "Vienna", "Minsk", "Brussels", "Sarajevo",
    "Sofia", "Zagreb", "Nicosia", "Prague", "Copenhagen", "Tallinn",
    "Helsinki", "Paris", "Berlin", "Athens", "Budapest", "Reykjavik", "Dublin",
    "Rome", "Pristina", "Riga", "Vaduz", "Vilnius", "Luxembourg", "Skopje",
    "Valletta", "Chisinau", "Monaco", "Podgorica", "Amsterdam", "Oslo",
    "Warsaw", "Lisbon", "Bucharest", "Moscow", "San Marino", "Belgrade",
    "Bratislava", "Ljubljana", "Madrid", "Stockholm", "Bern", "Ankara",
    "Kyiv", "London", "Vatican City"
]

def get_city_info(city):
    base_wait_time = 1  # seconds
    max_attempts = 5

    api_city = f"https://api.api-ninjas.com/v1/city?name={city}"

    for attempt in range(max_attempts):
        try:
            city_request = requests.get(url=api_city, headers={'X-Api-Key': city_api_key})
            city_request.raise_for_status()
            city_json = city_request.json()
            return  {"longitude": city_json[0]['longitude'], "latitude": city_json[0]['latitude']}
        except requests.exceptions.HTTPError as e:
            wait_time = base_wait_time * (2 ** attempt)  # Exponential backoff
            logging.error(f"HTTPError occurred: {e}. Retrying in {wait_time}s...")
            time.sleep(wait_time)
        except Exception as i:
            logging.error(f"Unexpected error occurred: {i}")
            return None

    logging.error(f"Failed to get city info after {max_attempts} attempts.")
    return None

def get_weather_data(latitude, longitude, endpoint, meteo_api_key):
    url = f"https://api.openweathermap.org/data/2.5/{endpoint}?lat={latitude}&lon={longitude}&appid={meteo_api_key}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"Error getting weather data: {e}")
        return None

producer = Producer({
        "bootstrap.servers": "b-1.cluster1.8sekeq.c3.kafka.eu-west-3.amazonaws.com:9092, b-2.cluster1.8sekeq.c3.kafka.eu-west-3.amazonaws.com:9092",
        "acks": "all",
        "retries": "5"
    })

def delivery_report(err, msg):
    if err is not None:
        logging.error(f"Message delivery failed: {err}")
    else:
        logging.info(f"Message delivered to {msg.topic()} [ {msg.partition()} ]")

def send_message(city):
    city_info = get_city_info(city)
    if city_info is None:
        return None

    data = get_weather_data(city_info["latitude"], city_info["longitude"], "weather", meteo_api_key)
    if data is None:
        return None

    forecast_data = get_weather_data(city_info["latitude"], city_info["longitude"], "forecast", meteo_api_key)
    if forecast_data is None:
        return None

    pollution = get_weather_data(city_info["latitude"], city_info["longitude"], "air_p      ollution", meteo_api_key)
    if pollution is None:
        return None

    new_info = {
        "city": f"{city}",
        "country": f"{data['sys']['country']}",
        "time": f"{datetime.utcfromtimestamp(data['dt']).strftime('%Y-%m-%d %H:%M:%S')}",
        "weather": f"{data['weather'][0]['main']}",
        "description": f"{data['weather'][0]['description']}",
        "temp": f"{data['main']['temp']}",
        "air_quality": f"{pollution['list'][0]['main']['aqi']}",
        "5_days_forecast": f"{forecast_data['list'][0]['main']['temp']}"
    }
    return json.dumps(new_info).encode('utf-8')

while True:
    try:
        for city in european_capitals:
            msg = send_message(city)
            if msg is not None:
                producer.produce(value=msg, topic="weather", on_delivery=delivery_report)
            time.sleep(1)  # Pause for a while before processing the next city
    except Exception as e:
        logging.error(f"Error: {e}")
    finally:
        producer.flush()  # Ensure all messages have been sent
        logging.info(f"Processing completed.")
        print("Processing completed.")


