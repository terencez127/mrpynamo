import logging
import gevent
import sys
import zerorpc
from dynamo import DynamoClientNode
from Worker import Mapper
from Worker import Reducer

class TaskServer():
    def __init__(self, dynamo_server_file, master_file, addr):
        self.dynamo_client = DynamoClientNode(dynamo_server_file, addr)
        self.workers = []
        self.has_all_data = False
        self.masters = []

    def new_worker(self, task_id, src, is_mapper, input_key=None):
        if is_mapper:
            worker = Mapper()
        else:
            worker = Reducer()


        self.workers.append(worker)
        worker.execute()


TIMEOUT_CLIENT = 10

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    from gevent import monkey
    monkey.patch_all()

    addr = sys.argv[1]
    config_file = 'server_config'
    server = TaskServer(config_file, addr)
    s = zerorpc.Server(server)
    s.bind('tcp://' + addr)
    s.run()