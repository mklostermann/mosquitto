#!/usr/bin/env python

# Test whether a Kafka connection works. Needs a running Kafka broker at localhost:9092 (use 11-kafka-bridge_kafka.conf).

import inspect, os, sys

# From http://stackoverflow.com/questions/279237/python-import-a-module-from-a-folder
cmd_subfolder = os.path.realpath(os.path.abspath(os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"..")))
if cmd_subfolder not in sys.path:
    sys.path.insert(0, cmd_subfolder)

import mosq_test, logging
from kafka import KafkaConsumer

rc = 1
keepalive = 10
connect_packet = mosq_test.gen_connect("kafka-bridge-test", keepalive=keepalive)
connack_packet = mosq_test.gen_connack(rc=0)
publish_packet = mosq_test.gen_publish("test/kafka", qos=0, payload="kafka test message")

broker = mosq_test.start_broker(filename=os.path.basename(__file__))

# create a Kafka consumer
logging.getLogger('kafka').addHandler(logging.NullHandler())
consumer = KafkaConsumer(bootstrap_servers="localhost:9092", consumer_timeout_ms=10)
consumer.subscribe(pattern=".*")

try:
    sock = mosq_test.do_client_connect(connect_packet, connack_packet)
    sock.send(publish_packet)
    sock.close()
    rc = 0
finally:
    broker.terminate()
    broker.wait()
    if rc:
        (stdo, stde) = broker.communicate()
        print(stde)

# validate message sent to Kafka
message = next(consumer)
consumer.close()
if message.value != "kafka test message" or message.topic != "test.kafka":
    print "Test message was not correctly transmitted to Kafka consumer, received: {0}".format(message)
    rc = 1

exit(rc)

