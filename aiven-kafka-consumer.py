# This script receives messages from a Kafka topic
from json import loads
from json import dumps
from kafka import KafkaConsumer

consumer = KafkaConsumer(
                        "json-data-stream",
                        auto_offset_reset="earliest",
                        bootstrap_servers="kafka-2c20c13e-mfomenko-028c.aivencloud.com:22025",
                        client_id="consumer-1",
                        group_id="consumer-group-1",
                        security_protocol="SSL",
                        ssl_cafile="ca.cer",
                        ssl_certfile="service.cert",
                        ssl_keyfile="service.key",
                        key_deserializer=lambda x: loads(x.decode('utf-8')),
                        value_deserializer=lambda x: loads(x.decode('utf-8'))
                        )

# Call poll twice. First call will just assign partitions for our
# consumer without actually returning anything

for _ in range(2):
    raw_msgs = consumer.poll(timeout_ms=1000)
    for tp, msgs in raw_msgs.items():
        for msg in msgs:
            print("Received a message with: msgKey={}, msgData={}".format(dumps(msg.key), dumps(msg.value)))

# Commit offsets so we won't get the same messages again
consumer.commit()