import logging
from quixstreams import Application

def temp_transform(msg):
    celsius = msg["temperature"]
    fahrenheit = (celsius * 9 / 5) + 32
    kelvin = celsius + 273.15
    new_msg = {
        "celsius": celsius,
        "fahrenheit": round(fahrenheit, 2),
        "kelvin": round(kelvin, 2),
        "device_id": msg["device_id"],
        "timestamp": msg["timestamp"],
    }
    return new_msg

def alert(msg):
    kelvin = msg["kelvin"]
    if kelvin > 303:
        logging.error("🚨 Temperature too high!")
        return True
    else:
        return False

def main():
    logging.info("START...")
    app = Application(
        broker_address="localhost:9092",
        consumer_group="alert",
        auto_offset_reset="latest",
    )
    input_topic = app.topic("sensor", value_deserializer="json")
    input_topic = app.topic("sensor", value_deserializer="json")
    output_topic = app.topic("alert", value_serializer="json")


    sdf = app.dataframe(input_topic)
    sdf = sdf.apply(temp_transform)
    sdf = sdf.filter(alert)
    sdf.to_topic(output_topic)
    app.run(sdf)

if __name__ == "__main__":
    logging.basicConfig(level="DEBUG")
    main()