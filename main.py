from influxdb_client import InfluxDBClient, Point
from influxdb_client.client import write_api
import paho.mqtt.client as mqtt
import config
import json
import numbers
from datetime import datetime


client = InfluxDBClient(url=config.influxdb_url, token=config.token, org=config.org, timeout=config.timeout, verify_ssl=config.verify_ssl)
influx_write = client.write_api(write_options=write_api.SYNCHRONOUS)


def on_mqtt_connect(client, userdata, flags, rc):
    print("Connected to mqtt server with result code "+str(rc))
    client.subscribe(config.subscribe_topic)


def on_mqtt_message(client, userdata, msg):
    print("Received '%s' - '%s'" % (msg.topic, str(msg.payload)))
    try:
        data = json.loads(msg.payload)
        p = Point(msg.topic)
        for key in data:
            if key == "timestamp":
                if isinstance(data["timestamp"], numbers.Number):
                    p.time(data["timestamp"])
                else:
                    p.time(datetime.strptime(data["timestamp"], "%Y-%m-%d %H:%M:%S"))
            else:
                p.field(key, data[key])
        influx_write.write(bucket=config.bucket, record=p)
    except json.decoder.JSONDecodeError:
        print("### Message payload is invalid JSON! ###")


client = mqtt.Client()
client.on_connect = on_mqtt_connect
client.on_message = on_mqtt_message
client.connect(config.mqtt_host, config.mqtt_port, 60)
client.loop_forever()