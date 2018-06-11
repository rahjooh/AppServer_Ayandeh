import os
import uuid
import threading
import time
import string
import random
import json

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
    producer = KafkaProducer(bootstrap_servers=['kafka-server:9092'], compression_type='gzip')
    topic = "requests"
    size = 6
    temp = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(size))
    req_data = str.encode(temp)
    rand_id = str(uuid.uuid4())
    request_id = str.encode(rand_id)
    r = {'request_id':request_id, 'data':req_data}
    r = json.dumps(r)
    req_data = str(r)
    req_data = str.encode(req_data)
    ret = producer.send(topic, req_data)
    time.sleep(1)
    consumer = KafkaConsumer(bootstrap_servers=['kafka-server:9092'],auto_offset_reset='earliest')
    consumer.subscribe(['responses'])
    ret = ''
    for message in consumer:
        ret = message.value
        try:
            json_object = json.loads(ret)
            if(json_object['response_id'] != request_id):
                continue
            ret = json_object
        except ValueError, e:
            continue
        #print ret
        break
    return str(ret['data'])

if __name__ == "__main__":
    app.run(host='10.100.136.48',port=8000)
