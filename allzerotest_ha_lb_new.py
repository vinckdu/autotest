#!usr/bin/python
# -*- coding: utf-8 -*- 
"""
@author:Vinck.Du
@file: insert.py
@time: 2019/09/15
"""
import boto3
import botocore
import time
import threading
import random
import sys
import ssl
import commands
from datetime import datetime
try:
    ssl._create_default_https_context = ssl._create_unverified_context
except AttributeError:
    pass
except Exception as exc:
    print(exc)

access_key = 'S7ZU7LHXU6RQAE7AJ5ER'
secret_key = 'eEQK57nvEqhOPfU6dNbGONNpQRFB96MqMxp8lbYw'

def putobject(Bucket,Key,Body):
    dt=datetime.now()
    mytime = dt.strftime( '%Y-%m-%d %H:%M:%S %f' )
    print(mytime +"  put to 192.168.242.48")
    s3 = boto3.resource(
    service_name ='s3',
    aws_access_key_id = access_key,
    aws_secret_access_key = secret_key,
    endpoint_url = 'http://192.168.242.48:8082',
    verify = False
    )

    try:
        s3.Bucket(Bucket).put_object(Key=Key,Body=Body)
    except Exception as e:
        print("There is error when uploading")
        print(e)


myint = int(sys.argv[3])
for i in range(1,myint) :
    putobject(sys.argv[1],sys.argv[2]+"-"+str(i),"hello,world!"*10)
    print("Now objects "+sys.argv[1]+" "+ sys.argv[2]+"-"+str(i) +" created !")


