
@app.route('/3dclustering')
@cross_origin(origin='*',headers=['Content-Type','Authorization'])

def _3dclustering():
    ip = request.remote_addr
    producer = KafkaProducer(bootstrap_servers=['kafka-server:9092'], compression_type='gzip')
    topic = "requests4"
    rand_id = str(uuid.uuid4())
    request_id = str.encode(rand_id)
    r = {'request_id':request_id, 'action':'_3dclustering'}
    r = json.dumps(r)
    req_data = str(r)
    req_data = str.encode(req_data)
    ret = producer.send(topic, req_data)
    time.sleep(1)
    global cunsumer
    #consumer = KafkaConsumer(bootstrap_servers=['kafka-server:9092'],auto_offset_reset='earliest')
    #consumer.subscribe(['responses2'])
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

@app.route('/no_transaction_vs_amount')
@cross_origin(origin='*',headers=['Content-Type','Authorization'])

def no_transaction_vs_amount():
    ip = request.remote_addr
    producer = KafkaProducer(bootstrap_servers=['kafka-server:9092'], compression_type='gzip')
    topic = "requests4"
    rand_id = str(uuid.uuid4())
    request_id = str.encode(rand_id)
    r = {'request_id':request_id, 'action':'no_transaction_vs_amount'}
    r = json.dumps(r)
    req_data = str(r)
    req_data = str.encode(req_data)
    ret = producer.send(topic, req_data)
    time.sleep(1)
    print request_id
    global cunsumer
    #consumer = KafkaConsumer(bootstrap_servers=['kafka-server:9092'],auto_offset_reset='latest')
    #consumer.subscribe(['responses2'])
    ret = ''
    for message in consumer:
        ret = message.value
        print ret
        try:
            json_object = json.loads(ret)
            if(json_object['response_id'] != request_id):
                continue
            ret = json_object
        except ValueError, e:
            continue
        print ret
        break
    return str(ret['data'])

@app.route('/no_transaction_vs_harmonic')
@cross_origin(origin='*',headers=['Content-Type','Authorization'])

def no_transaction_vs_harmonic():
    ip = request.remote_addr
    producer = KafkaProducer(bootstrap_servers=['kafka-server:9092'], compression_type='gzip')
    topic = "requests4"
    rand_id = str(uuid.uuid4())
    request_id = str.encode(rand_id)
    r = {'request_id':request_id, 'action':'no_transaction_vs_harmonic'}
    r = json.dumps(r)
    req_data = str(r)
    req_data = str.encode(req_data)
    ret = producer.send(topic, req_data)
    time.sleep(1)
    global cunsumer
    #consumer = KafkaConsumer(bootstrap_servers=['kafka-server:9092'],auto_offset_reset='earliest')
    #consumer.subscribe(['responses2'])
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

@app.route('/amount_vs_harmonic')
@cross_origin(origin='*',headers=['Content-Type','Authorization'])

def amount_vs_harmonic():
    ip = request.remote_addr
    producer = KafkaProducer(bootstrap_servers=['kafka-server:9092'], compression_type='gzip')
    topic = "requests4"
    rand_id = str(uuid.uuid4())
    request_id = str.encode(rand_id)
    r = {'request_id':request_id, 'action':'amount_vs_harmonic'}
    r = json.dumps(r)
    req_data = str(r)
    req_data = str.encode(req_data)
    ret = producer.send(topic, req_data)
    time.sleep(1)
    global cunsumer
    #consumer = KafkaConsumer(bootstrap_servers=['kafka-server:9092'],auto_offset_reset='earliest')
    #consumer.subscribe(['responses2'])
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

@app.route('/label_counts')
@cross_origin(origin='*',headers=['Content-Type','Authorization'])

def label_counts():
    ip = request.remote_addr
    producer = KafkaProducer(bootstrap_servers=['kafka-server:9092'], compression_type='gzip')
    topic = "requests4"
    rand_id = str(uuid.uuid4())
    request_id = str.encode(rand_id)
    r = {'request_id':request_id, 'action':'label_counts'}
    r = json.dumps(r)
    req_data = str(r)
    req_data = str.encode(req_data)
    ret = producer.send(topic, req_data)
    time.sleep(1)
    global cunsumer
    #consumer = KafkaConsumer(bootstrap_servers=['kafka-server:9092'],auto_offset_reset='earliest')
    #consumer.subscribe(['responses2'])
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

@app.route('/no_transactions_statics')
@cross_origin(origin='*',headers=['Content-Type','Authorization'])

def no_transactions_statics():
    ip = request.remote_addr
    producer = KafkaProducer(bootstrap_servers=['kafka-server:9092'], compression_type='gzip')
    topic = "requests4"
    rand_id = str(uuid.uuid4())
    request_id = str.encode(rand_id)
    r = {'request_id':request_id, 'action':'no_transactions_statics'}
    r = json.dumps(r)
    req_data = str(r)
    req_data = str.encode(req_data)
    ret = producer.send(topic, req_data)
    time.sleep(1)
    global cunsumer
    #consumer = KafkaConsumer(bootstrap_servers=['kafka-server:9092'],auto_offset_reset='earliest')
    #consumer.subscribe(['responses2'])
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

@app.route('/amount_statics')
@cross_origin(origin='*',headers=['Content-Type','Authorization'])

def amount_statics():
    ip = request.remote_addr
    producer = KafkaProducer(bootstrap_servers=['kafka-server:9092'], compression_type='gzip')
    topic = "requests4"
    rand_id = str(uuid.uuid4())
    request_id = str.encode(rand_id)
    r = {'request_id':request_id, 'action':'amount_statics'}
    r = json.dumps(r)
    req_data = str(r)
    req_data = str.encode(req_data)
    ret = producer.send(topic, req_data)
    time.sleep(1)
    global cunsumer
    #consumer = KafkaConsumer(bootstrap_servers=['kafka-server:9092'],auto_offset_reset='earliest')
    #consumer.subscribe(['responses2'])
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

@app.route('/harmonic_statics')
@cross_origin(origin='*',headers=['Content-Type','Authorization'])

def harmonic_statics():
    ip = request.remote_addr
    producer = KafkaProducer(bootstrap_servers=['kafka-server:9092'], compression_type='gzip')
    topic = "requests4"
    rand_id = str(uuid.uuid4())
    request_id = str.encode(rand_id)
    r = {'request_id':request_id, 'action':'harmonic_statics'}
    r = json.dumps(r)
    req_data = str(r)
    req_data = str.encode(req_data)
    ret = producer.send(topic, req_data)
    time.sleep(1)
    global cunsumer
    #consumer = KafkaConsumer(bootstrap_servers=['kafka-server:9092'],auto_offset_reset='earliest')
    #consumer.subscribe(['responses2'])
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
