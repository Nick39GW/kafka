from datetime import datetime
from quixstreams import Application
import streamlit as st
from collections import deque
import json

temperature_buffer = deque(maxlen=100)
timestamp_buffer = deque(maxlen=100)

st.title("Real-Time IoT Dashboard")

@st.cache_resource
def kafka_connection():
    return Application(
        broker_address="localhost:9092",
        consumer_group="dashboard",
        auto_offset_reset="latest",
    )

app = kafka_connection()
sensor_topic = app.topic("sensor")
alert_topic = app.topic("alert")
avg_temp_topic = app.topic("avg_temperature_10s", value_deserializer="json")
alerts_count_topic = app.topic("alerts_count_5s", value_deserializer="json")

# Create columns once
col_left, col_right = st.columns(2)

# Placeholders for metrics
metric_avg_temp = col_left.empty()
metric_alerts_count = col_right.empty()

# Placeholders for sensor data outside columns
st_metric_temp = st.empty()
st_chart = st.empty()

with app.get_consumer() as consumer:
    consumer.subscribe([
        sensor_topic.name,
        alert_topic.name,
        avg_temp_topic.name,
        alerts_count_topic.name
    ])

    previous_temp = 0

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue

        topic = msg.topic()

        if topic == sensor_topic.name:
            sensor_msg = sensor_topic.deserialize(msg)
            temperature = sensor_msg.value.get('temperature')
            device_id = sensor_msg.value.get('device_id')
            timestamp = datetime.fromisoformat(sensor_msg.value.get('timestamp'))
            diff = temperature - previous_temp
            previous_temp = temperature
            timestamp_str = timestamp.strftime("%H:%M:%S")
            st_metric_temp.metric(label=device_id, value=f"{temperature:.2f} °C", delta=f"{diff:.2f} °C")
            timestamp_buffer.append(timestamp_str)
            temperature_buffer.append(temperature)
            st_chart.line_chart(
                data={"time": list(timestamp_buffer), "temperature": list(temperature_buffer)},
                x="time",
                y="temperature",
                use_container_width=True,
            )

        elif topic == avg_temp_topic.name:
            avg_msg = json.loads(msg.value().decode("utf-8"))
            avg_temp = avg_msg.get("avg_temperature", None)
            if avg_temp is not None:
                metric_avg_temp.metric(label="Avg Temp (10s)", value=f"{avg_temp:.2f} °C")
            else:
                metric_avg_temp.metric(label="Avg Temp (10s)", value="N/A")

        elif topic == alerts_count_topic.name:
            alert_msg = json.loads(msg.value().decode("utf-8"))
            alert_count = alert_msg.get("count", None)
            if alert_count is not None:
                metric_alerts_count.metric(label="Alerts (5s)", value=alert_count)
            else:
                metric_alerts_count.metric(label="Alerts (5s)", value="N/A")
