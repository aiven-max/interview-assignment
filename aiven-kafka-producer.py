# This script produces messages to a Kafka topic
from time import sleep
from json import dumps
import uuid
from random import randrange
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers="kafka-2c20c13e-mfomenko-028c.aivencloud.com:22025",
                         security_protocol="SSL",
                         ssl_cafile="ca.cer",
                         ssl_certfile="service.cert",
                         ssl_keyfile="service.key",
                         key_serializer=lambda x: dumps(x).encode('utf-8'),
                         value_serializer=lambda x: dumps(x).encode('utf-8')
                        )

# Generate 5 IoT messages, write them to console and push to Kafka Topic
for e in range(5):
    msgKey = str(uuid.uuid4())
    msgData = {"motorId": "Fulton-A32", "eventTs": datetime.now().isoformat(), "sensorDataEvent": {"pressure": 20+randrange(10), "temperature": 40+randrange(5)}}
    producer.send('json-data-stream', key=msgKey, value=msgData)
    print("Sent a message with msgKey={} msgData={}".format(dumps(msgKey),dumps(msgData)))
    sleep(1)

# Ensure buffer is flushed and all messages are indeed sent to Kafka Broker 
producer.flush()