import logging
import sys
import zerorpc
from dynamo import DynamoNode


logging.basicConfig(level=logging.ERROR)

from gevent import monkey
monkey.patch_all()

addr = sys.argv[1]
# config_file = sys.argv[2]
config_file = 'config_master'
server = DynamoNode(config_file, addr)
s = zerorpc.Server(server)
s.bind('tcp://' + addr)
s.run()