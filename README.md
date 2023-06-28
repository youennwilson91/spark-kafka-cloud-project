# 1st-spark-kafka-cloud-pipeline

### Introduction 
This is my first project. The goal is to build a reliable, scalable and secure pipeline and push it to the cloud. The extra will be automation with airflow and dockerisation for compatibility. There won't be any transformation made on the data, since I already know how spark.sql works. I will update this README as I am going forward with the project. Every change made won't be commited to the main branch so we will be able to compare the initial project from the final product.

In the future, I might decide to merge this data with data from another project as part of a bigger project.

### Import

Here are the dependencies needed for this script.

import time
import requests
from datetime import datetime
from confluent_kafka import Producer
import json
import logging

Note that for confluent_kafka, you'll have to execute pip install confluent-kafka. There must be an error somewhere in the installation process, you won't be able to use confluent kafka if you put an underscore in the installation command 

Let's moveon with our first script: the data producer.

### Producer.py 

This script aims to fetch data from two APIs and send it to the AWS MSK broker. The two APIs are:
  - https://api.api-ninjas.com. This API will give us the latitude and the longitude of european cities.
  - https://api.openweathermap.org. This API will give us the information on a city such as the current weather, air condition and a forecast to 5 days.

Thus, we need two API keys and a list of cities. We will take european capitals in our case.

```

    city_api_key = "YOUR_NINJA_API_KEY"
    meteo_api_key = "YOUR_OPENWEATHER_API_KEY"
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
        base_wait_time = 1   # Base wait time for exponential backoff
        max_attempts = 5 # Maximum number of attempts to make

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
        base_wait_time = 1 
        max_attempts = 5  

        url = f"https://api.openweathermap.org/data/2.5/{endpoint}?lat={latitude}&lon={longitude}&appid={meteo_api_key}"

        for attempt in range(max_attempts):
            try:
                response = requests.get(url)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as e: 
                wait_time = base_wait_time * (2 ** attempt)  # Exponential backoff
                logging.error(f"HTTPError occurred: {e}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            except Exception as e:
                logging.error(f"Error getting weather data: {e}")
                continue

        logging.error(f"Failed to get weather data after {max_attempts} attempts.")
        return None

```

Now that we define functions to get our data, we can create a producer that will send messages to kafka. I kept the producer configurations to the minimum.

```
    producer = Producer({
        "bootstrap.servers": "b-1.cluster1.8sekeq.c3.kafka.eu-west-3.amazonaws.com:9092, b-2.cluster1.8sekeq.c3.kafka.eu-west-3.amazonaws.com:9092",
        "acks": "all",
        "retries": "5"
    })	
```

We also define a function that will serve as a callback. This will allow us to know if the message has been delivered successfuly.

```
    def delivery_report(err, msg):
        if err is not None:
            logging.error(f"Message delivery failed: {err}")
        else:
            logging.info(f"Message delivered to {msg.topic()} [ {msg.partition()} ]")
```

Then define our last function that will shape the city climate information message that will be sent to kafka. 
The function takes one argument: the city we want to send information about.
Thanks to the first api call that gives us city locations, we can get these cities current weather, forecast and air condition. All this information is stored in a dictionary called "new_info". 
The function thus return the new_info dictonary under an utf-8 encoded json form.

```

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
```

Finally, we set up a while loop that goes on until it is manually terminated. This loop will go through our european cities list, and send a climate information message to kafka for each city.

```
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
```

Here is our kafka producer. For each city, it gets the location, the climate information then shape all that into a json encoded message that will be sent to kafka. 
