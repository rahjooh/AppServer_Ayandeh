import sys
import time
import os
import zlib
import kafka

from kafka import KafkaConsumer
consumer = KafkaConsumer('req',bootstrap_servers=['kafka-server:9092'])
for request in consumer:
    temp = request.value
    t = zlib.decompress(temp)
    print t
    #zcontent = zlib.compress(content)
