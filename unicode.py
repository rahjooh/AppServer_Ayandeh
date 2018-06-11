import json, pprint, requests, textwrap
# import pandas as pd
from io import StringIO
import time

host = "http://10.100.136.13:8070"
headers = {'Content-Type': 'application/json; charset=utf-8'}


def start_session():
    data = {'kind': 'pyspark'}
    response = requests.post(host + '/sessions', data=json.dumps(data), headers=headers)
    start_response = response.json()

    session_id = start_response[ "id" ]
    session_state = start_response[ "state" ]

    return session_id, session_state


def get_all_sessions():
    response = requests.get(host + '/sessions', headers=headers)
    return response.json()


def delete_session(session_id):
    response = requests.delete(host + '/sessions/' + str(session_id), headers=headers)
    return response.json()


def get_session_state(session_id):
    response = requests.get(host + "/sessions/" + str(session_id), headers=headers)
    session_response = response.json()
    session_state = session_response[ "state" ]
    return session_state


def run_code(code_data):

    try:
        session_id, session_state = start_session()
    except Exception as exc:
        raise exc
    print(session_id)
    while session_state == "starting":
        session_state = get_session_state(session_id)

    post_response = requests.post(host + "/sessions/" + str(session_id) + "/statements",
                                  data=json.dumps(code_data),
                                  headers=headers)

    response = post_response.json()
    state = response[ "state" ]
    statement_id = response[ "id" ]
    while state != "available":
        get_response = requests.get(host + "/sessions/" + str(session_id) + "/statements/" + str(statement_id),
                                    headers=headers)
        response = get_response.json()
        # response.content.decode('utf-8')
        time.sleep(0.001)
        try:
            state = response["state"]
        except:
            break

        #print(state)
    if state == "available":
        # jsonContent = json.dumps(response, ensure_ascii=True)
        # print(type(jsonContent))
        # myjs = json.loads(jsonContent)
        # s1 = myjs['output']['data']['text/plain']
        # print(type(s1))
        # jsonContent = json.dumps(s1, ensure_ascii=True)
        # print(type(s1))
        # myjs = json.loads(jsonContent)
        # print(myjs[0]['name1'])
        response_status = response[ "output" ][ "status" ]
        if response_status == "error":
            error = response[ "output" ][ "evalue" ]
            print(error)
        else:
            delete_session(session_id)
            response_dict = response[ "output" ][ "data" ]
            # clean_response_dict = response_dict["text/plain"].strip("\\").strip("'")
            # response_dict = response_dict[0].encode('latin1').decode('utf8')
            # map_to(response_dict)
            stt = response_dict['text/plain'].strip("\\").strip("'")
            myjs = json.loads(stt)
            # my_str = myjs['name1']["0"].replace('\\','\\\\')
            # my_str = my_str = u"{}".format(my_str)
            print(bytes(myjs['name1']['0'] ,"utf-8").decode('unicode-escape'))
            # print(u', '.join(x.decode('unicode-escape') for x in myjs['name1']['0']))
            # print(str(u'{0}'.format(myjs['name1']['0'].replace('\\','\\\\'))))
            # myjs = json.loads(clean_response_dict)
            # print(type(myjs))
            # print(myjs['name1']["0"])
            # dataframe = pd.read_json(resp)
            # print(dataframe)
            # dataframe = dataframe.set_index("merchant_number")
def map_to(d):
    # iterate over the key/values pairings
    for k, v in d.items():
        # if v is a list join and encode else just encode as it is a string
        d[k] = ",".join(v).encode("utf-8") if isinstance(v, list) else v.encode("utf-8")

def stop_using_unicode(x):
    def unicode_to_str(x):
        return x.normalize("NFKD").encode("ascii", "ignore")

    return {unicode_to_str(k): unicode_to_str(v) for k, v in x.items()}
def send_to_livy():

    code_data = {
        'code': textwrap.dedent("""
                from pyspark.sql.types import *
                myschema = StructType([StructField('name1',StringType(),True),StructField('name2',StringType(),True),StructField('name3',StringType(),True)])
                DF = spark.read.format("com.databricks.spark.csv").option("header","false").schema(myschema).option("delimiter",',').load("hdfs://10.100.136.13:9000/user/hduser/unicode/*")
                pDF = DF.toPandas()
                pDF.to_json()
                """.format())
    }
    return run_code(code_data)

if __name__ == "__main__":
    send_to_livy()