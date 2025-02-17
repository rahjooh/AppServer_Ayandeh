import os
import uuid
import threading
import time
import string
import random
import json
import zlib

from flask import Flask, Response
from flask import request, jsonify
#from flask.ext.cors import CORS, cross_origin
from flask_cors import CORS, cross_origin
from flask_compress import Compress
from flask import send_file
import pyexcel as pe
import StringIO

from flask import Flask, make_response
from kafka import KafkaProducer
from kafka import KafkaConsumer

# init flask app
app = Flask(__name__)
Compress(app)
app.config['CORS_HEADERS'] = 'Content-Type'

kafkaServer = "kafka-server:9092"

requestTopicAnalysis = "requests_topic"
responseTopicAnalysis = "responses_topic"
requestTopicGetinfo = "requestsinfo_topic"
responseTopicGetinfo = "responsesinfo_topic"

def plot(method, action, data):
    actionList = [
        'guild_analysis',
        '3dclustering',
        'no_transaction_vs_amount',
        'no_transaction_vs_harmonic',
        'amount_vs_harmonic',
        'label_counts',
        'boxplot_num',
        'boxplot_amount',
        'boxplot_rec',
    ]
    if action not in actionList:
        print("action does not exist")
        return "action does not exist"

    request_id = str(uuid.uuid4())
    if method == 'POST':
        if request.data != '':
            data = json.loads(str(request.data))
            r = {'request_id': request_id, 'action': action, 'data': data}
        else:
            r = {'request_id': request_id, 'action': action}
    else:
        r = {'request_id': request_id, 'action': action}

    r = json.dumps(r)
    req_data = str.encode(str(r))

    producer = KafkaProducer(bootstrap_servers=[kafkaServer], compression_type='gzip')
    topic = requestTopicAnalysis
    ret = producer.send(topic, req_data)
    print(ret)
    time.sleep(1)

    print("request '" + str(request_id) + "' has been produced.")
    print("consumer of request id '" + str(request_id) + "' starting ...")
    consumer = KafkaConsumer(responseTopicAnalysis,bootstrap_servers=[kafkaServer], auto_offset_reset='latest')
    # consumer.subscribe([])
    # global cunsumer
    ret = ''
    for message in consumer:
        temp = message.value
        # print(temp)
        try:
            ret = zlib.decompress(temp)
        except zlib.error:
            temp = temp
        try:
            json_object = json.loads(ret)
            # print(json_object)
            if (str(json_object['response_id']) != str(request_id)):
                print(str(json_object['response_id']) + " no match")
                continue
            ret = json_object
            break
        except ValueError, e:
            continue

    print("--------")
    # print(ret)
    return str(ret['data'])

# getInfo
@app.route('/getInfo',methods=['GET','POST'])
@cross_origin(origin='*',headers=['Content-Type','Authorization'])
def getInfo():
    print("new request to build CSV file - getInfo")
    data = json.loads(str(request.data))
    id = data[0][0]
    # id = "__236__"
    request_id = str(uuid.uuid4())
    r = {'request_id': request_id, 'action': 'getinfo', 'cluster_id':id, 'boxplot_name':'boxplot_amount'}
    r = json.dumps(r)
    req_data = str.encode(str(r))

    producer = KafkaProducer(bootstrap_servers=[kafkaServer], compression_type='gzip')
    topic = requestTopicGetinfo
    ret = producer.send(topic, req_data)
    print(ret)
    time.sleep(1)

    print("consumer of request id '" + str(request_id) + "' starting ...")
    consumer = KafkaConsumer(responseTopicGetinfo,bootstrap_servers=[kafkaServer],auto_offset_reset='earliest')
    ret = ''
    for message in consumer:
        temp = message.value
        try:
            ret = zlib.decompress(temp)
        except zlib.error:
            temp = temp
        try:
            json_object = json.loads(ret)
            if (str(json_object['response_id']) != str(request_id)):
                print(str(json_object['response_id']) + " no match")
                continue
            ret = json_object
            break
        except ValueError, e:
            continue
    print(ret)
    data = str(ret['data'])
    sheet = pe.Sheet(data)
    io = StringIO.StringIO()
    sheet.save_to_memory("csv", io)

    output = make_response(io.getvalue())

    output.headers["Content-Disposition"] = "attachment; filename=export.csv"
    output.headers["Content-type"] = "text/csv"
    return output
    # #return send_file("Adjacency.csv", as_attachment=True, mimetype='application/vnd.ms-excel')

# toCSV
@app.route('/getPlotCSV',methods=['GET','POST'])
@cross_origin(origin='*',headers=['Content-Type','Authorization'])
def getPlotCSV():
    print("new request to build CSV file")
    data = json.loads(str(request.data))
    sheet = pe.Sheet(data)
    io = StringIO.StringIO()
    sheet.save_to_memory("csv", io)

    output = make_response(io.getvalue())
    output.headers["Content-Disposition"] = "attachment; filename=export.csv"
    output.headers["Content-type"] = "text/csv"
    #return send_file("Adjacency.csv", as_attachment=True, mimetype='application/vnd.ms-excel')
    return output

# guild_analysis
@app.route('/guild_analysis',methods=['GET','POST'])
@cross_origin(origin='*',headers=['Content-Type','Authorization'])
def guild_analysis():
    print("new request to guild_analysis")
    cont = request.get_json()
    return Response(plot(request.method, 'guild_analysis', request.data), status=200, mimetype='text/plain')

# 3dclustering
@app.route('/3dclustering',methods=['GET','POST'])
@cross_origin(origin='*',headers=['Content-Type','Authorization'])
def _3dclustering():
    print("new request to 3dclustering")
    return Response(plot(request.method, '3dclustering', request.data), status=200, mimetype='text/plain')

# no_transaction_vs_amount
@app.route('/no_transaction_vs_amount',methods=['GET','POST'])
@cross_origin(origin='*',headers=['Content-Type','Authorization'])
def no_transaction_vs_amount():
    print("new request to no_transaction_vs_amount")
    return Response(plot(request.method, 'no_transaction_vs_amount', request.data), status=200, mimetype='text/plain')

# no_transaction_vs_harmonic
@app.route('/no_transaction_vs_harmonic',methods=['GET','POST'])
@cross_origin(origin='*',headers=['Content-Type','Authorization'])
def no_transaction_vs_harmonic():
    print("new request to no_transaction_vs_harmonic")
    return Response(plot(request.method, 'no_transaction_vs_harmonic', request.data), status=200, mimetype='text/plain')

# amount_vs_harmonic
@app.route('/amount_vs_harmonic',methods=['GET','POST'])
@cross_origin(origin='*',headers=['Content-Type','Authorization'])
def amount_vs_harmonic():
    print("new request to amount_vs_harmonic")
    return Response(plot(request.method, 'amount_vs_harmonic', request.data), status=200, mimetype='text/plain')

# label_counts
@app.route('/label_counts',methods=['GET','POST'])
@cross_origin(origin='*',headers=['Content-Type','Authorization'])
def label_counts():
    print("new request to label_counts")
    return Response(plot(request.method, 'label_counts', request.data), status=200, mimetype='text/plain')

# boxplot_num
@app.route('/boxplot_num',methods=['GET','POST'])
@cross_origin(origin='*',headers=['Content-Type','Authorization'])
def boxplot_num():
    print("new request to boxplot_num")
    return Response(plot(request.method, 'boxplot_num', request.data), status=200, mimetype='text/plain')

# boxplot_num
@app.route('/boxplot_amount',methods=['GET','POST'])
@cross_origin(origin='*',headers=['Content-Type','Authorization'])
def boxplot_amount():
    print("new request to boxplot_amount")
    return Response(plot(request.method, 'boxplot_amount', request.data), status=200, mimetype='text/plain')

# boxplot_rec
@app.route('/boxplot_rec',methods=['GET','POST'])
@cross_origin(origin='*',headers=['Content-Type','Authorization'])
def boxplot_rec():
    print("new request to boxplot_rec")
    return Response(plot(request.method, 'boxplot_rec', request.data), status=200, mimetype='text/plain')


with open('./log', 'a+') as log:
    try:
        app.run(host='10.100.136.47', port=8000, threaded=True)
        log.write("done adding wsgi app\n")
    except Exception, e:
        log.write(repr(e))
