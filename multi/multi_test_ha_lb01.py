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
import thread
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

access_key = 'NZF839DRD334XA15A36A'
secret_key = 'jdxX6JDXOgqXCUXRhtAv8DyRbGBJ2WawsOUqlEH5'

def putobject(Endpoint,Bucket,Key,Body):
    dt=datetime.now()
    mytime = dt.strftime( '%Y-%m-%d %H:%M:%S %f' )
    s3 = boto3.resource(
    service_name ='s3',
    aws_access_key_id = access_key,
    aws_secret_access_key = secret_key,
    endpoint_url = 'http://192.168.242.16:8082',
    verify = False
    )

    try:
        s3.Bucket(Bucket).put_object(Key=Key,Body=Body)
    except Exception as e:
        print("There is error when uploading")
        print(e)

def onethread(Threadname,Endpoint,Bucket,Key,Body,Objnum):
    Objnum=int(Objnum)
    for i in range(0,Objnum) :
        myint=str(i)
        putobject(Endpoint,Bucket,Threadname+" "+Key+myint,Body*1000)
        print(Threadname+" "+Bucket+" object  "+Key+myint+"  created")

threadnum=int(sys.argv[6])
for i in range(0,threadnum):
    thread.start_new_thread(onethread,("mythread"+"-"+str(i),sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4],sys.argv[5]))
while True:
    pass
