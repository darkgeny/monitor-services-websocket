from pyhive import hive
import time
import pandas as pd
import sys
import subprocess
import requests
import json
import warnings
import sqlalchemy
from urllib.parse import unquote
import html

warnings.filterwarnings('ignore')

from flask import Flask, jsonify, request

HIVE_DISCONNECTED=True
conn=""

app = Flask(__name__)

tasks = [
    {
        'id': 0
    }
]

def hiveconnect():
    global conn,HIVE_DISCONNECTED
    if HIVE_DISCONNECTED == True:
        try:
            conn = hive.Connection(host="bdata2", auth='NOSASL')
        except Exception as e:
            print("Hive connection is dead.")
            print(e)
            HIVE_DISCONNECTED=True
            return conn
    HIVE_DISCONNECTED=False
    print("Hive connection is alive:",conn)
    return conn

conn = hiveconnect()

@app.route('/hive', methods=['GET'])
def get_hive():
    global HIVE_DISCONNECTED
    hivestatus="reconnecting"
    hiveconnect()
    try:
        df = pd.read_sql("SELECT * FROM monitor", conn)
    except Exception as e:
        HIVE_DISCONNECTED=True
        return jsonify({"service_name": "hive", "status": "down", "log": hivestatus})
        
    if str(df.head()) == "   monitor.a\n0          1":
        return jsonify({"service_name": "hive", "status":"active"})
    else:
        return jsonify({"service_name": "hive", "status":"down"})

@app.route('/process', methods=['GET'])
def get_process():
    cmd = unquote(request.args.get('cmd'))
    #cmd = ["/bin/bash","-i","-c","/opt/cluster/checkprocs/exec-chk-procs.sh","kylin"]
#    cmd = ["/bin/bash","-i","-c","ls -al"]
    cmd = ["/bin/bash","-i","-c",cmd," 2>&1"," & "]
    print("cmd",cmd)
    try:
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        #exitcode,output = subprocess.getstatusoutput(cmd)
#        output=subprocess.getoutput('ls /bin/ls')
    #    output = subprocess.check_output(cmd, stderr=subprocess.STDOUT,shell=True)
        #print("exitcode:",exitcode)
        print("output:",output)
        a=str(output)
        b=a[1:-1].strip()
        c="<br>"+b[1:len(b)]
        return jsonify({"command": cmd,"result":  html.escape(c)})
    except Exception as e:
        print("error:",e)
    finally:
        pass
    return jsonify({"service_name": "process", "status":"down"})

@app.route('/namenode', methods=['GET'])
def get_namenode():
    try:
        response = requests.get("http://bdata1:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus")
    except requests.exceptions.RequestException as e:
        return jsonify({'namenode':{'status':'down'}})
    namenode=response.json()
    try:
        j=jsonify({"service_name": "namenode", "status":namenode['beans'][0]['State']})
        return j
    except:
        return jsonify({'namenode':{'status':'down'}})


@app.route('/datanodes', methods=['GET'])
def get_datanodes():
    try:
        response = requests.get("http://bdata1:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo")
    except requests.exceptions.RequestException as e:
        return jsonify({"service_name": "datanodes", "status":"down"})
    datanodes=response.json()
    try:
        d=json.loads(datanodes['beans'][0]['LiveNodes'])
    except:
        return jsonify({"service_name": "datanodes", "status":"down"})

    res={}
    for k in d:
        state=d[k]['adminState']
        if state == 'In Service':
            state='active'
        res[k]={'status':state}
    return jsonify(res)

@app.route('/hbase', methods=['GET'])
def get_hbase():
    try:
        response = requests.get("http://bdata4:16010/jmx")
    except requests.exceptions.RequestException as e:
        return jsonify({"service_name": "hbase", "status":"down"})
    hbase=response.json()
    for h in hbase['beans']:
        if h['name'] == 'Hadoop:service=HBase,name=Master,sub=Server':
            check=h["tag.liveRegionServers"]
            break
    if check[:5] == "bdata":
        hb={"service_name": "hbase", "status":"active"}
    else:
        hb={"service_name": "hbase", "status":"down"}
    return jsonify(hb)
#hbase=hbase[:-1]
    #hbase=hbase.strip()
    #json.loads("{"+hbase+"}")
    #return "{"+hbase+"}"

@app.route('/kylin', methods=['GET'])
def get_kylin():
    url = "http://bdata3:7070/kylin/api/user/authentication"
    header = {"Authorization" : "Basic QURNSU46S1lMSU4="}
    try:
        response = requests.get(url, headers=header, timeout=5)
        r=response.json()
        if r['userDetails']['username'] == 'ADMIN':
            return jsonify({"service_name": "kylin", "status":"active"})
        else:
            return jsonify({"service_name": "kylin", "status":"down","code":response.status_code})
    except requests.exceptions.RequestException as e:
        return jsonify({"service_name": "kylin", "status":"down"})
    # sys.exit(141)

@app.route('/all', methods=['GET'])
def call_myself_get_all():
    r2=connect("http://localhost:5000/datanodes")
    r0={"services": []}
    r0["services"].append({"service_name":{"datanodes": r2}})
    r1=connect("http://localhost:5000/namenode")
    r3=connect("http://localhost:5000/hbase")
    r4=connect("http://localhost:5000/hive")
    r5=connect("http://localhost:5000/kylin")
#    r={**r1, **r2, **r3, **r4, **r5, **r6}
    r0["services"].append(r1)
    r0["services"].append(r3)
    r0["services"].append(r4)
    r0["services"].append(r5)
    r=r0
    return jsonify([r])
def connect(url):
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException as e:
        return jsonify({'connection':'failed'})
    return response.json()

if __name__ == '__main__':
    app.run(host="0.0.0.0",debug=True)
