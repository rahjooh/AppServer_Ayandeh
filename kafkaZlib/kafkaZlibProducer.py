import sys
import time
import os
import zlib
import kafka

from kafka import KafkaProducer
content = open('valid.txt', 'r').read()
print str(len(content))
zcontent = zlib.compress(content)
print str(len(zcontent))
#print zcontent
producer = KafkaProducer(bootstrap_servers=["kafka-server:9092"], compression_type='gzip')
topic = "req"
#data = str.encode(content)
producer.send(topic, zcontent)
time.sleep(1)
