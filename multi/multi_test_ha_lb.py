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

def putobject(Threadname,Endpoint,Bucket,Key,Body):
    dt=datetime.now()
    mytime = dt.strftime( '%Y-%m-%d %H:%M:%S %f' )
    print(mytime +"  put to "+Endpoint)
    print("here we go")
    s3 = boto3.resource(service_name ='s3',aws_access_key_id = access_key,aws_secret_access_key = secret_key,endpoint_url = 'http://'+Endpoint+':8082',verify = False)
    s3.Bucket(Bucket).put_object(Key=Key,Body=Body*1000)
    try:
        print(Threadname+ " put to " + Endpoint)
        s3.Bucket(Bucket).put_object(Key=Key,Body=Body*1000)
    except Exception as e:
        print("There is error when uploading")
        print(e)
    print("upload completed")

def onethread(Threadname,Endpoint,Bucket,Key,Body,Objnum):
    print("start onethread")
    dt=datetime.now()
    mytime = dt.strftime( '%Y-%m-%d %H:%M:%S %f' )
    print(mytime +"  put to "+Endpoint)
    s3 = boto3.resource(
    service_name ='s3',
    aws_access_key_id = access_key,
    aws_secret_access_key = secret_key,
    endpoint_url = 'http://'+Endpoint+':8082',
    verify = False
    )
    print("Now here")
    print("iamhere")
    for i in range(0,Objnum) :
        myint=str(i)
        try:
            s3.Bucket(Bucket).put_object(Key=Key+myint,Body=Body)
            print(Threadname + " "+Key+" ")
        except Exception as e:
            print("There is error when uploading")
            print(e)
        print(Threadname+" "+Bucket+" object  "+Key+myint+"  created")

def demothread(threadname):
    print(threadname)

threadnum=int(sys.argv[5])
print(threadnum)
print(type(threadnum))
for i in range(0,threadnum):
    print(i)
    tn="mytread"+str(i)
    #thread.start_new_thread(demothread,("test-",))
    #thread.start_new_thread(putobject,("192.168.242.16","vinckbucket","hello","haha"))
    # putobject("192.168.242.16","vinckbucket","hello","haha")
    # thread.start_new_thread(demothread,("thread",))
    thread.start_new_thread(putobject,(tn,sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4],))
while True:
    pass
