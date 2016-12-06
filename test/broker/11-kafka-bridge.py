#!/usr/bin/env python

# Test whether a Kafka connection works. Needs a running Kafka broker at localhost:9092 (use 11-kafka-bridge_kafka.conf).

import inspect, os, sys

# From http://stackoverflow.com/questions/279237/python-import-a-module-from-a-folder
cmd_subfolder = os.path.realpath(os.path.abspath(os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"..")))
if cmd_subfolder not in sys.path:
    sys.path.insert(0, cmd_subfolder)

import mosq_test
from kafka import KafkaConsumer

rc = 1
keepalive = 10
connect_packet = mosq_test.gen_connect("kafka-bridge-test", keepalive=keepalive)
connack_packet = mosq_test.gen_connack(rc=0)
publish_packet = mosq_test.gen_publish("test/kafka", qos=0, payload="kafka test message")

broker = mosq_test.start_broker(filename=os.path.basename(__file__))

try:
    sock = mosq_test.do_client_connect(connect_packet, connack_packet)
    sock.send(publish_packet)
    sock.close()
    consumer = KafkaConsumer('test.kafka', metadata_broker_list='localhost:9092')
    msg = next(consumer)
    print msg
    if msg.value == "kafka test message":
        rc = 0
finally:
    broker.terminate()
    broker.wait()
    if rc:
        (stdo, stde) = broker.communicate()
        print(stde)

exit(rc)

