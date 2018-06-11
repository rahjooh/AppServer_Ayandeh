import sys
from kafka import KafkaProducer
from kafka import KafkaConsumer
import time
import json
import zlib
import random
import threading
from random import randint
import plotly
from plotly.offline import plot as offpy
import plotly.graph_objs as go

kafkaServer = "kafka-server:9092"
requestTopic = "requests_topic"
responseTopic = "response_topic"

class AnalysisThread(object):
    def __init__(self, action, request_id, request_data):
        self.interval = 1
        thread = threading.Thread(target=self.run, args=(action, request_id, request_data))
        thread.daemon = True
        thread.start()
    def run(self, action, request_id, request_data):
        print("req_id" + str(request_id))
        actionList = [
            'guild_analysis',
            'heatmap',
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
            # send response false
        #
        # t = random.randint(1, 3)
        # data = t
        print("wait for 1 sec...")
        # time.sleep(data)
        x = ['senf1', 'senf2', 'senf3']
        text = ['node1', 'node2', 'node3']
        fill = ['info1', 'info2', 'info3']
        y1 = randint(1, 10)
        y2 = randint(1, 10)
        y3 = randint(1, 10)
        y = [y1, y2, y3]
        data = bar_chart_plot(x, y, text, fill, 'random')
        # data = "text message"
        print(send_response(request_id, result=True, data=data, message=""))
        print("finish")
def bar_chart_plot(x, y, text, info, title):
    trace1 = go.Bar(
        x=x,
        y = y,
        text = text,
        name = info,
    )
    data = [ trace1 ]
    layout = go.Layout(title=title)
    fig = go.Figure(data=data, layout=layout)
    return offpy(fig,include_plotlyjs=False,show_link=False,output_type='div')

def send_response(request_id, result, data=None, message=None):

    with open("requests_id.txt", 'a') as fw:
        fw.write(str(request_id)+"\n")

    response_dict = {'response_id': request_id, 'result': result, 'data': data, 'message': message}
    response_json = json.dumps(response_dict)

    topic = responseTopic
    compressData = True
    if compressData:
        content = str(response_json)
        zcontent = zlib.compress(str.encode(str(content)))
        ret = producer.send(topic, zcontent)
    else:
        response_bytes = str.encode(str(response_json))
        ret = producer.send(topic, response_bytes)

    time.sleep(1)
    return ret


producer = KafkaProducer(bootstrap_servers=[kafkaServer], compression_type='gzip')
def start():
    consumer = KafkaConsumer('request-topic', bootstrap_servers=[ 'kafka-server:9092' ], auto_offset_reset="latest")
    for request in consumer:

        request_dict = json.loads(request.value)
        action = request_dict[ 'action' ]
        print("action name : " + action)
        request_data = ""
        if "data" in request_dict:
            request_data = request_dict['data']

        request_id = request_dict[ 'request_id' ]

        if request_id in list(open("requests_id.txt").readline()):
            print("repetetive!")
            continue

        print(request_dict)
        if action == "guild_analysis":
            print("process guild_analysis ...")
            t = AnalysisThread('heatmap', request_id, request_data)
        elif action == "3dclustering":
            print("process 3dclustering ...")
            t = AnalysisThread('3dclustering', request_id, request_data)
        elif action == "no_transaction_vs_amount":
            print("process no_transaction_vs_amount ...")
            t = AnalysisThread('no_transaction_vs_amount', request_id, request_data)
        elif action == "no_transaction_vs_harmonic":
            print("process no_transaction_vs_harmonic ...")
            t = AnalysisThread('no_transaction_vs_harmonic', request_id, request_data)

        else:
            send_response(request_id, result=False, message="action not found")

if __name__ == "__main__":
    start()

