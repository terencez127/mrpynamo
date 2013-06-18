import random
import gevent
from dynamo import DynamoClientNode
import logging
import sys
import zerorpc
from dynamo import DynamoNode


class MRServer():
    class MRJob():
        def __init__(self, input_list, output_dir, src):
            self.input_list = input_list
            self.output_dir = output_dir
            self.src = src
            self.mappers = []
            self.reducers = []


    def __init__(self, server_file, workers_file, addr):
        self.dynamo_client = DynamoClientNode(server_file, addr)
        # Store jobs' information
        self.jobs = []
        self.workers = []
        self.worker_liveness = []
        self.available_workers = set()

    def new_job(self, input_list, output_dir, src):
        for i in input_list:
            while len(self.available_workers) == 0:
                gevent.sleep(0.01)
            worker = self.available_workers.pop()


    def run_job(self, job):

        pass

    def start(self):
        pass

    def periodically_func(self):
        for i, worker in enumerate(self.workers):
            try:
                ret = worker.are_you_there()
            except zerorpc.TimeoutExpired:
                self.worker_liveness[i] = False
                if worker in self.available_workers:
                    self.available_workers.remove(worker)
        gevent.sleep(2)



TIMEOUT_CLIENT = 10

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    from gevent import monkey
    monkey.patch_all()

    addr = sys.argv[1]
    config_file = 'server_config'
    server = MRServer(config_file, addr)
    s = zerorpc.Server(server)
    s.bind('tcp://' + addr)
    s.run()