import os
import uuid
import threading
import time
import string
import random
import json

from flask import Flask, Response
from flask import request, jsonify
#from flask.ext.cors import CORS, cross_origin
from flask_cors import CORS, cross_origin
from flask_compress import Compress

from kafka import KafkaProducer
from kafka import KafkaConsumer


# global variable

kafkaServer = "kafka-server:9092"
requestTopic = "requests5"
responseTopic = "responses5"

consumer = KafkaConsumer(bootstrap_servers=[kafkaServer],auto_offset_reset='latest')
consumer.subscribe([responseTopic])
def func(method, action, data):
    actionList = [
        'guild_analysis',
        '3dclustering',
        'no_transaction_vs_amount',
        'no_transaction_vs_harmonic',
        'amount_vs_harmonic',
        'label_counts',
        'no_transactions_statics',
        'amount_statics',
        'harmonic_statics',
    ]
    if action not in actionList:
        print("action does not exist")
        return "action does not exist"

    request_id = str(uuid.uuid4())
    if method == 'POST':
        # data = json.loads(str(request.data))
        # data = {'date_from': 9,'date_to':11}
        # data = {'date_from':str(post_data['min']),'date_to':str(post_data['max'])}
        r = {'request_id': request_id, 'action': action, 'data': data}
    else:
        r = {'request_id': request_id, 'action': action}

    r = json.dumps(r)
    req_data = str.encode(str(r))

    producer = KafkaProducer(bootstrap_servers=[kafkaServer], compression_type='gzip')
    topic = requestTopic
    ret = producer.send(topic, req_data)
    time.sleep(1)

    print("request '" + str(request_id) + "' has been produced.")
    print("consumer of request id '" + str(request_id) + "' starting ...")
    consumer = KafkaConsumer(bootstrap_servers=[kafkaServer], auto_offset_reset='latest')
    consumer.subscribe([responseTopic])
    # global cunsumer
    ret = ''
    for message in consumer:
        ret = message.value
        try:
            json_object = json.loads(ret)
            print(json_object)
            print("--------")
            if (str(json_object['response_id']) != str(request_id)):
                continue
            ret = json_object
            print(ret)
        except ValueError, e:
            continue
        print(ret)
        break
    return str(ret['data'])

# init flask app
app = Flask(__name__)
Compress(app)
app.config['CORS_HEADERS'] = 'Content-Type'

# flask route

@app.route('/guild_analysis',methods=['GET','POST'])
@cross_origin(origin='*',headers=['Content-Type','Authorization'])
def guild_analysis():
    print("new request")
    d = threading.Thread(name='daemon', target=func(request.method,'guild_analysis','data1'))
    d.setDaemon(True)
    d.start()
    return Response(d.join(), status=200, mimetype='text/plain')

if __name__ == "__main__":
    app.run(host='10.100.136.47',port=8000)
