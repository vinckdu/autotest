#!usr/bin/python2.7
# -*- coding: utf-8 -*-
"""
@author:Vinck.Du
@file: multithreaduploadobj.py
@time: 2019/09/15
"""
import boto3
import botocore
import time
import threading
import random
import sys
import ssl
from datetime import datetime

access_key = 'FGNMDCS360XGQJFW1VL2'
secret_key = 'sTaOihNUFt2mMxAbNAQWo88WqfCDCKKfZ9WaAGFt'
totalcount=0

mytime=time.time()
filename=str(mytime)+".log"
with open(filename,"a+") as log_file:
    pass


def putobject(Endpoint, Bucket, Key, Body):

    global totalcount
    dt = datetime.now()
    mytime = dt.strftime('%Y-%m-%d %H:%M:%S %f')

    try:
        # s3.Bucket(Bucket).put_object(Key=Key,Body=Body)
        #GLACIER
        starttime = time.time()
        timestamp = str(time.strftime("%Y%m%d%H%M%S"))
        newkey = Key+"_"+timestamp
        Bucket.put_object(Key=newkey,Body=Body)
        stoptime=time.time()
        delay=stoptime-starttime
        mytimestamp=time.strftime("%Y-%m-%d %H:%M:%S  ")
        #log_file=file(filename,"a+")
        with open(filename, "a+") as log_file:
            log=" object  "+newkey+"  created. timedelay:"+str(delay*1000)+"ms\n"
            log_file.write(mytimestamp)
            log_file.write(log)
        #print(mytimestamp+log)
    except Exception as e:
        mytimestamp=time.strftime("%Y-%m-%d %H:%M:%S  ")
        log=str(e)
        with open(filename, "a+") as log_file:
            log_file.write(mytimestamp)
            log_file.write(log+"\n")
        print("There is error when uploading")
        print(e)

def onethread(Threadname,Endpoint,Bucket,Key,Body,Objnum):
    global totalcount
    Objnum=int(Objnum)
    s3 = boto3.resource(
    service_name ='s3',
    aws_access_key_id = access_key,
    aws_secret_access_key = secret_key,
    endpoint_url = 'http://'+Endpoint+':8082',
    verify = False
    )
    Bkt=s3.Bucket(Bucket)
    body=Body*1024
    for i in range(0,Objnum) :
        myint=str(i)
        putobject(Endpoint,Bkt,Bucket+Threadname+Key+myint,body)
        with threading.Lock():
           totalcount += 1
       # print(Threadname+"test"+str(totalcount))

        mytimestamp=time.strftime("%Y-%m-%d %H:%M:%S  ")
        #log_file=file(filename,"a+")
        #log=Threadname+" : "+Bucket+" object  "+Threadname+Key+myint+"  created\n"
       # log_file.write(mytimestamp)
      #  log_file.write(log+"\n")
     #   log_file.close()
        #print(mytimestamp+" "+ Threadname+" : "+Bucket+" object  "+Threadname+Key+myint+"  created\n")

threadnum=int(sys.argv[6])
for i in range(0,threadnum):
    time.sleep(0.2)
    threading.Thread(target=onethread, args=("thread"+str(i),sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4],sys.argv[5])).start()
while True:
    pass
    begin = totalcount
    time.sleep(10)
    mytimestamp=time.strftime("%Y-%m-%d %H:%M:%S  ")
    print(str(mytimestamp) +"  totalcount "+ str(totalcount))
    IOPS=(totalcount-begin)/10
    print("*************************当前10S内的IOPS***************************: "+str(IOPS))

