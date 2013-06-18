#!/usr/bin/env python
#coding=utf-8
import sys
import zerorpc

from dynamo import DynamoClientNode

job_name = sys.argv[1]

with open('config_master', 'r') as f:
    master_list = [i.strip() for i in f.readlines()]

connections = []
for i, server in enumerate(master_list):
    c = zerorpc.Client(timeout=10)
    c.connect('tcp://' + server)
    connections.append(c)

# print key_list
# key_list = ['short' + str(i) for i in xrange(slices)]
try:
    ret = connections[0].kill_job(job_name)
    if ret:
        print 'Job ' + job_name + ' is killed!'
except:
    print 'Request failed!'
