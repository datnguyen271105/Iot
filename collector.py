
import json
import os
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# ===== INFLUXDB =====
INFLUX_URL = "http://localhost:8086"
INFLUX_ORG = "IoT_Lab"
INFLUX_BUCKET = "iot_raw"
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "n_nKL-QwZqM1Lw01KvK0wly__crqa3LGDQcc2Q7JTwPr2KiXi8jTFW69lHN6YJx1lgk4ZiovJOhopgtQwNbF-A==")

# ===== MQTT =====
MQTT_HOST = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPIC = "BTH2/NguyenTienDat/B23DCCN141"
DEVICE_ID = "ESP32_B23DCCN141"

db = InfluxDBClient(
    url=INFLUX_URL,
    token=INFLUX_TOKEN,
    org=INFLUX_ORG
)
write_api = db.write_api(write_options=SYNCHRONOUS)

received_count = 0
invalid_count = 0
last_sequence = None


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print("MQTT connected")
        client.subscribe(MQTT_TOPIC, qos=0)
        print("Subscribed:", MQTT_TOPIC)
    else:
        print("MQTT connection failed:", reason_code)


def on_message(client, userdata, msg):
    global received_count, invalid_count, last_sequence

    received_at = datetime.now(timezone.utc)

    try:
        data = json.loads(msg.payload.decode("utf-8"))

        # Validate payload
        if data.get("device_id") != DEVICE_ID:
            raise ValueError("Unexpected device_id")

        required = ["temperature", "humidity", "distance_cm",
                    "rssi", "sequence", "uptime_s"]

        if any(k not in data for k in required):
            raise ValueError("Missing required field")

        temperature = float(data["temperature"])
        humidity = float(data["humidity"])
        distance = float(data["distance_cm"])
        rssi = int(data["rssi"])
        sequence = int(data["sequence"])
        uptime_s = int(data["uptime_s"])

        if not (-40 <= temperature <= 80):
            raise ValueError("Temperature out of range")
        if not (0 <= humidity <= 100):
            raise ValueError("Humidity out of range")
        if not (2 <= distance <= 400):
            raise ValueError("Distance out of range")

        # Device timestamp: epoch milliseconds
        timestamp_ms = data.get("timestamp_ms")
        valid_timestamp = (
            isinstance(timestamp_ms, (int, float))
            and not isinstance(timestamp_ms, bool)
            and timestamp_ms > 0
        )

        if valid_timestamp:
            event_time = datetime.fromtimestamp(
                timestamp_ms / 1000, tz=timezone.utc
            )
            latency_ms = (
                received_at - event_time
            ).total_seconds() * 1000

            # Reject implausible timestamps for latency calculation.
            # Still store the sensor data using receive time.
            if not (0 <= latency_ms <= 24 * 60 * 60 * 1000):
                latency_ms = None
        else:
            event_time = received_at
            latency_ms = None

        point = (
            Point("sensor_raw")
            .tag("device_id", DEVICE_ID)
            .field("temperature", temperature)
            .field("humidity", humidity)
            .field("distance_cm", distance)
            .field("rssi", rssi)
            .field("sequence", sequence)
            .field("uptime_s", uptime_s)
            .time(event_time, WritePrecision.MS)
        )

        if latency_ms is not None:
            point.field("latency_ms", float(latency_ms))

        write_api.write(
            bucket=INFLUX_BUCKET,
            org=INFLUX_ORG,
            record=point
        )

        received_count += 1

        if last_sequence is not None:
            gap = sequence - last_sequence
            if gap > 1:
                print(f"Possible missing messages: {gap - 1}")
            elif gap <= 0:
                print("Duplicate or out-of-order sequence:", sequence)

        last_sequence = sequence

        print(
            f"Saved #{sequence}: T={temperature:.2f} C, "
            f"H={humidity:.2f} %, D={distance:.2f} cm, "
            f"latency={latency_ms}"
        )

    except (ValueError, TypeError, KeyError, json.JSONDecodeError) as e:
        invalid_count += 1
        print("Invalid payload:", e)

    except Exception as e:
        print("Processing/write error:", repr(e))


client = mqtt.Client(
    callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    client_id=f"python-iot-{int(time.time())}"
)
client.on_connect = on_connect
client.on_message = on_message
client.reconnect_delay_set(min_delay=1, max_delay=30)

try:
    # Keep the collector running and reconnect on transient failures.
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    print("Collector started. Waiting for sensor data...")
    client.loop_forever()
except KeyboardInterrupt:
    print("\nStopping collector...")
finally:
    client.disconnect()
    write_api.close()
    db.close()
    print("Valid records:", received_count)
    print("Invalid records:", invalid_count)
