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
import multiprocessing
from multiprocessing import Value, Lock

access_key = 'FGNMDCS360XGQJFW1VL2'
secret_key = 'sTaOihNUFt2mMxAbNAQWo88WqfCDCKKfZ9WaAGFt'
totalcount = Value('i',-1)

mytime=time.time()
filename = str(mytime)+".log"
#with open(filename,"a+") as log_file:
#    pass


def myadd(totalcount,lock):
    for i in range(0,10000):
        with lock:
            totalcount.value +=1
            print(totalcount.value)
        time.sleep(1)

def putobject(Endpoint, Bucket, Key, Body):

    dt=datetime.now()
    mytime = dt.strftime( '%Y-%m-%d %H:%M:%S %f' )

    try:
        # s3.Bucket(Bucket).put_object(Key=Key,Body=Body)
        #GLACIER
        starttime=time.time()
        timestamp=str(time.strftime("%Y%m%d%H%M%S"))
        newkey=Key+"_"+timestamp
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

def onethread(Threadname,Endpoint,Bucket,Key,Body,Objnum,totalcount, lock):
    Objnum=int(Objnum)
    s3 = boto3.resource(
    service_name ='s3',
    aws_access_key_id = access_key,
    aws_secret_access_key = secret_key,
    endpoint_url = 'http://'+Endpoint+':8082',
    verify = False
    )
    Bkt = s3.Bucket(Bucket)
    body = Body*1024
    for i in range(0, Objnum):
        myint = str(i)
        putobject(Endpoint,Bkt,Bucket+Threadname+Key+myint,body)
        with lock:
           totalcount.value +=1
           #print("Current totalcount: ",totalcount)
       # print(Threadname+"test"+str(totalcount))

        mytimestamp=time.strftime("%Y-%m-%d %H:%M:%S  ")
        #log_file=file(filename,"a+")
        #log=Threadname+" : "+Bucket+" object  "+Threadname+Key+myint+"  created\n"
       # log_file.write(mytimestamp)
      #  log_file.write(log+"\n")
     #   log_file.close()
        #print(mytimestamp+" "+ Threadname+" : "+Bucket+" object  "+Threadname+Key+myint+"  created\n")

def stastics(totalcount):
    while True:
        pass
        with multiprocessing.Lock():
            begin = totalcount.value
            print("Get the lock")
            print("Total count " + str(begin))
        time.sleep(10)
        mytimestamp = time.strftime("%Y-%m-%d %H:%M:%S  ")
        with multiprocessing.Lock():
            end = totalcount.value
        print(str(mytimestamp) +"  totalcount "+ str(end))
        IOPS=(end-begin)/10
        print("*************************当前10S内的IOPS***************************: "+str(IOPS))


def main(*args):

    threadnum = int(args[6])
    for i in range(0,threadnum):
        #multiprocessing.Process(target=onethread, args=("thread"+str(i),args[1],args[2],args[3],args[4],args[5])).start()
        multiprocessing.Process(target=myadd,args=()).start()
    while True:
        pass
        with multiprocessing.Lock():
            begin = totalcount.value
            print("Get the lock")
            print("Total count " + str(begin))
        time.sleep(10)
        mytimestamp = time.strftime("%Y-%m-%d %H:%M:%S  ")
        with multiprocessing.Lock():
            end = totalcount.value
        print(str(mytimestamp) + "  totalcount " + str(end))
        IOPS = (end - begin) / 10
        print("*************************当前10S内的IOPS***************************: " + str(IOPS))
        print("Python is really fun")


if __name__ =='__main__':
    # sys.exit(main(sys.argv[0],sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4],sys.argv[5],sys.argv[6]))
    lock = Lock()
    totalcount = Value("i",0)
    threadnum = int(sys.argv[6])
    for i in range(0, threadnum):
        multiprocessing.Process(target=onethread, args=("thread"+str(i),sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4],sys.argv[5],totalcount, lock)).start()
     #   multiprocessing.Process(target=myadd, args=(totalcount, lock)).start()
    while True:
        begin = totalcount.value
        print("Total count " + str(begin))
        time.sleep(10)
        mytimestamp = time.strftime("%Y-%m-%d %H:%M:%S  ")
        end = totalcount.value
        print(str(mytimestamp) + "  totalcount " + str(end))
        IOPS = (end - begin) / 10
        print("*************************当前10S内的IOPS***************************: " + str(IOPS))

