import os
import string
import random
import threading
import time
from flask import Flask
from flask import request, jsonify
#from flask.ext.cors import CORS, cross_origin
from flask_cors import CORS, cross_origin

from kafka import KafkaProducer
from kafka import KafkaConsumer

app = Flask(__name__)
cors = CORS(app, resources={r"/foo": {"origins": "*"}})
app.config['CORS_HEADERS'] = 'Content-Type'
@app.route('/',methods=['GET','POST'])
def index():
    if request.method == 'GET':
        return "index of service GET"
    if request.method == 'POST':
        return "index of service POST"

@app.route('/bar1')
@cross_origin(origin='*',headers=['Content-Type','Authorization'])

def bar1():
    ip = request.remote_addr
    producer = KafkaProducer(bootstrap_servers=['kafka-server:9092'])
    topic = "requests"
    temp = ''.join(random.choice(''.join(random.choice(chars) for _ in range(size))) for _ in range(size))
    data = str.encode()
    ret = producer.send(topic, ip)
    time.sleep(1)
    consumer = KafkaConsumer(bootstrap_servers=['kafka-server:9092'],auto_offset_reset='earliest')
    consumer.subscribe(['responses'])
    ret = ''
    for message in consumer:
        ret = message.value
        break
    return "request send and get resp:" + str(ret)

if __name__ == "__main__":
    app.run(host='10.100.136.48',port=8000)
