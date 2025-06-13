import json
from datetime import datetime, timedelta, timezone
from quixstreams import Application

app = Application(broker_address="localhost:9092")

input_topic = app.topic("alert", value_deserializer="json")
output_topic = app.topic("alerts_count_5s", value_serializer="json")

consumer = app.get_consumer()
consumer.subscribe([input_topic.name])
producer = app.get_producer()

alert_times = []

print("🚨 Alert Count KPI running...")

while True:
    msg = consumer.poll(timeout=1.0)
    if msg is None:
        continue

    if msg.topic() == input_topic.name:
        data = input_topic.deserialize(msg).value

        now = datetime.now(timezone.utc)
        alert_times.append(now)

        cutoff = now - timedelta(seconds=5)
        alert_times = [t for t in alert_times if t > cutoff]

        count = len(alert_times)

        producer.produce(
            topic=output_topic.name,
            key="alert_count",
            value=json.dumps({
                "count": count,
                "timestamp": now.isoformat()
            }).encode("utf-8")
        )
