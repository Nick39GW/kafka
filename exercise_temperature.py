import json
from datetime import datetime, timedelta, timezone
from quixstreams import Application

app = Application(broker_address="localhost:9092")

input_topic = app.topic("sensor", value_deserializer="json")
output_topic = app.topic("avg_temperature_10s", value_serializer="json")

consumer = app.get_consumer()
consumer.subscribe([input_topic.name])
producer = app.get_producer()

temperature_data = []

print("🌡️ Temperature KPI pipeline running...")

while True:
    msg = consumer.poll(timeout=1.0)
    if msg is None:
        continue

    if msg.topic() == input_topic.name:
        data = input_topic.deserialize(msg).value

        temperature = data.get("temperature")
        timestamp_str = data.get("timestamp")
        if temperature is None or timestamp_str is None:
            continue

        timestamp = datetime.fromisoformat(timestamp_str).replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)

        temperature_data.append((timestamp, temperature))
        cutoff = now - timedelta(seconds=10)
        temperature_data = [(ts, temp) for ts, temp in temperature_data if ts > cutoff]

        if temperature_data:
            avg_temp = sum(t for _, t in temperature_data) / len(temperature_data)

            kafka_msg = output_topic.serialize(
                key="avg_temp",
                value={
                    "avg_temperature": round(avg_temp, 2),
                    "timestamp": now.isoformat()
                }
            )

            producer.produce(
                topic=output_topic.name,
                key=kafka_msg.key,
                value=kafka_msg.value,
            )
          