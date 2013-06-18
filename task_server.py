import logging
import gevent
import sys
import zerorpc
from dynamo import DynamoClientNode
import importlib
import cPickle as pickle


class Worker:
    def __init__(self, task_id, src_func, output_func=None, output_loc=None, reducer_num=1):
        self.task_id = task_id
        self.src_func = src_func
        self.output_func = output_func
        self.output_loc = output_loc
        self.reducer_num = reducer_num
        self.greenlet = None
        # True if all data has been sent to this worker
        self.has_all_data = False

class TaskServer():
    def __init__(self, dynamo_server_file, master_file, addr):
        self.dynamo_client = DynamoClientNode(addr, dynamo_server_file)
        self.workers = {}
        self.pool = gevent.pool.Group()

        with open(master_file, 'r') as f:
            self.masters = [line.strip() for line in f.readlines()]

        self.connections = []
        for i, server in enumerate(self.masters):
            c = zerorpc.Client(timeout=10)
            c.connect('tcp://' + server)
            self.connections.append(c)

    def new_worker(self, task_id, src, is_mapper, input_key=None, output_loc=None, reducer_num=1):
        task_id = tuple(task_id)
        logging.info('new_worker')
        src_string = self.dynamo_client.get(src)[0]
        if not src.endswith('.py'):
            src += '.py'
        with open(src, 'w+') as f:
            f.write(src_string)
        src_mod = importlib.import_module(src[:-3])

        if is_mapper:
            mapper = src_mod.MyMapper(self.dynamo_client)
            worker = Worker(task_id, mapper.map)
        else:
            reducer = src_mod.MyReducer(self.dynamo_client)
            worker = Worker(task_id, reducer.reduce, reducer.output, output_loc)
            # print 'output_loc, worker.output', output_loc, worker.output_loc
        self.workers[task_id] = worker

        if is_mapper:
            self.workers[task_id].greenlet = self.pool.spawn(self.run_mapper, task_id, input_key)

    def no_more_data(self, task_id):
        task_id = tuple(task_id)
        logging.info('no_more_data')
        #TODO: Do we need to spawn it?
        worker = self.workers[task_id]
        worker.has_all_data = True

    def run_reducer(self, task_id, key_list):
        logging.info('send_data_to_reducer')
        task_id = tuple(task_id)
        self.workers[task_id].greenlet = \
            self.pool.spawn(self.reducing, task_id, key_list)

    def reducing(self, task_id, key_list):
        logging.info('send_all_data')
        task_id = tuple(task_id)
        for i in xrange(len(key_list)):
            data = pickle.loads(self.dynamo_client.get(key_list[i])[0])
            self.workers[task_id].src_func(data)

    def output_to_storage(self, location, data):
        logging.info('output_to_storage')
        self.dynamo_client.put(location, [], data)

    def run_mapper(self, task_id, input_key):
        logging.info('run_mapping')
        task_id = tuple(task_id)
        worker = self.workers[task_id]
        ret_list = worker.src_func(self.dynamo_client.get(input_key[0])[0])
        partition_dict = {}
        reducer_num = worker.reducer_num
        file_list = []
        for i, value in enumerate(ret_list):
            if i % reducer_num not in partition_dict:
                partition_dict[i % reducer_num] = []
            partition_dict[i % reducer_num].append(value)
        for i in partition_dict.items():
            file_name = task_id[0]+'/inter' + str(i[0]) + str(task_id[1])
            # print 'inter_list', i[1]
            self.dynamo_client.put(file_name, [], pickle.dumps(i[1]))
            file_list.append(file_name)

        for server in self.connections:
            try:
                ret = server.on_mapper_finish(task_id, file_list)
                if ret:
                    break
            except zerorpc.TimeoutExpired:
                print 'One master is down, try another one.'

    def kill_worker(self, task_id):
        if task_id in self.workers:
            if self.workers[task_id].greenlet is not None \
                    and not self.workers[task_id].greenlet.dead:
                self.pool.killone(self.workers[task_id].greenlet)
            del self.workers[task_id]


    def periodically_func(self):
        while True:
            for i in self.workers:
                worker = self.workers[i]
                if worker.has_all_data and worker.output_func is not None\
                    and worker.greenlet is not None\
                    and worker.greenlet.dead:
                    result = worker.output_func()
                    self.output_to_storage(worker.output_loc, result)
                    for server in self.connections:
                        try:
                            ret = server.on_reducer_finish(worker.task_id)
                            worker.greenlet = None
                            if ret:
                                break
                        except zerorpc.TimeoutExpired:
                            print 'One master is down, try another one.'
            gevent.sleep(2)

    def start(self):
        self.pool.spawn(self.periodically_func)

    def are_you_there(self):
        return True

TIMEOUT_CLIENT = 10

if __name__ == '__main__':
    # logging.basicConfig(level=logging.ERROR)

    from gevent import monkey
    monkey.patch_all()

    addr = sys.argv[1]
    dyanmo_file = 'config_dynamo'
    master_file = 'config_master'
    server = TaskServer(dyanmo_file, master_file, addr)
    s = zerorpc.Server(server)
    s.bind('tcp://' + addr)
    server.start()
    s.run()