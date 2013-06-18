import random
import gevent
import time
from dynamo import DynamoClientNode
import logging
import sys
import zerorpc

JOB_STATUS_DEFUALT = 0
JOB_STATUS_RUNNING = 1
JOB_STATUS_COMPLETED = 2
JOB_STATUS_INTERRUPTED = 3

TIMEOUT_CLIENT = 10

t1 = None
t2 = None

class Job():
    def __init__(self, name, input_list, output_dir, src):
        # Input file list in dynamo system
        self.input_list = input_list
        # Output directory in dynamo system
        self.output_dir = output_dir
        # Source file location
        self.src = src
        # Element: [task_server, task_id, is_done]
        self.mappers = []
        # Element: [task_server, task_id, is_done]
        self.reducers = []
        self.status = JOB_STATUS_DEFUALT
        self.reducer_number = 1
        self.name = name
        self.inter_files = {}

    def set_reducer_num(self, num):
        self.reducer_number = num

    def add_mapper(self, mapper):
        self.mappers.append(mapper)

    def add_reducer(self, mapper):
        self.reducers.append(mapper)


class Master():
    def __init__(self, dynamo_file, master_file, task_server_file, addr):
        self.dynamo_client = DynamoClientNode(addr, dynamo_file)
        # Store jobs' information
        self.jobs = {}
        with open(task_server_file, 'r') as f:
            self.task_servers = [line.strip() for line in f.readlines()]
        with open(master_file, 'r') as f:
            self.replicas = [line.strip() for line in f.readlines()]
        self.addr = addr

        self.task_server_liveness = [True for _ in self.task_servers]
        #TODO: monitor the usage of each server
        self.available_task_servers = set(self.task_servers)

        self.connections_replicas = []
        self.connections_task_server = []
        for i, server in enumerate(self.replicas):
            if server == self.addr:
                self.i = i
                self.connections_replicas.append(self)
            else:
                c = zerorpc.Client(timeout=10)
                c.connect('tcp://' + server)
                self.connections_replicas.append(c)

        for i, server in enumerate(self.task_servers):
            c = zerorpc.Client(timeout=10)
            c.connect('tcp://' + server)
            self.connections_task_server.append(c)

    def new_job(self, name, input_list, output_dir, src):
        logging.info('new_job')
        #TODO: check src name, don't contain period
        #TODO: check src format, must contain MyMapper and MyReducer

        # The name has been used
        if name in self.jobs:
            return False

        # print 'output_dir: ', output_dir
        j = Job(name, input_list, output_dir, src)
        self.jobs[name] = j
        return True

    def run_job(self, name):
        logging.info('run_job')
        global t1
        t1 = time.time()

        if name not in self.jobs:
            return False

        j = self.jobs[name]

        if j.status == JOB_STATUS_DEFUALT:
            j.status = JOB_STATUS_RUNNING

            # Initialize reducers
            for i in xrange(j.reducer_number):
                server = random.choice(list(self.available_task_servers))
                #(task_name, sequence, is_mapper)
                task_id = (name, i, False)
                conn = self.connections_task_server[self.task_servers.index(server)]
                j.add_reducer([server, task_id, False])
                # print 'j.output_dir', j.output_dir
                conn.new_worker(task_id, j.src, False, None, j.output_dir)

            # Initialize mappers
            for i in xrange(len(j.input_list)):
                # local_server = [n for n in self.available_task_servers
                #                 if n.startswith(j.input_list[i][1][0].split(':')[0])]
                local_server = []
                # print 'local_server', local_server
                if len(local_server) > 0:
                    mapper = local_server[0]
                else:
                    mapper = list(self.available_task_servers)[i%len(self.available_task_servers)]
                    # mapper = random.choice(list(self.available_task_servers))
                #(task_name, sequence, is_mapper)
                task_id = (name, i, True)
                j.add_mapper([mapper, task_id, False])

            for i in j.mappers:
                conn = self.connections_task_server[self.task_servers.index(i[0])]
                conn.new_worker(i[1], j.src, True, j.input_list[i[1][1]], j.reducer_number)

    def on_reducer_finish(self, task_id):
        logging.info('on_reducer_finish')
        if task_id[0] in self.jobs:
            job = self.jobs[task_id[0]]
            job.reducers[task_id[1]][2] = True
            # All reducers have done their work
            if len([i for i in job.reducers if not i[2]]) == 0:
                print 'Job:' + task_id[0] + ' completed!'
                job.status = JOB_STATUS_COMPLETED
                global t1
                print time.time() - t1
        return True


    def on_mapper_finish(self, task_id, file_list):
        logging.info('on_mapper_finish')
        job = self.jobs[task_id[0]]
        job.mappers[task_id[1]][2] = True
        for i, f in enumerate(file_list):
            if i not in job.inter_files:
                job.inter_files[i] = []
            job.inter_files[i].append(f)
            #TODO: send files to reducer before all mappers are done
        if len([i for i in job.mappers if not i[2]]) == 0:
            logging.info('begin reduce')
            print job.reducers
            for i, r in enumerate(job.reducers):
                index = self.task_servers.index(r[0])
                self.connections_task_server[index].run_reducer(
                    r[1], job.inter_files[r[1][1]])
                self.connections_task_server[index].no_more_data(r[1])

        return True

    def kill_job(self, name):
        try:
            for i, job in enumerate(self.jobs):
                if job.name == name:
                    job.status = JOB_STATUS_INTERRUPTED
                    for m in job.mappers:
                        index = self.task_servers.index(m[0])
                        self.connections_task_server[index].kill_worker(m[1])
                    for r in job.reducers:
                        index = self.task_servers.index(r[0])
                        self.connections_task_server[index].kill_worker(r[1])
                    del self.jobs[i]
                    return True
        except:
            pass
        return False

    def is_done(self, name):
        for job in self.jobs:
            if job.name == name and job.status == JOB_STATUS_COMPLETED:
                return True
        return False

    def periodically_func(self):
        while True:
            for i, server in enumerate(self.task_servers):
                try:
                    ret = self.connections_task_server[i].are_you_there()
                    self.task_server_liveness[i] = True
                    #TODO: monitor usage of task server
                    self.available_task_servers.add(server)
                except zerorpc.TimeoutExpired:
                    self.task_server_liveness[i] = False
                    if server in self.available_task_servers:
                        self.available_task_servers.remove(server)
            gevent.sleep(2)

    def start(self):
        gevent.spawn(self.periodically_func)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    from gevent import monkey
    monkey.patch_all()

    dynamo_file = 'config_dynamo'
    master_file = 'config_master'
    task_server_file = 'config_task_server'
    addr = sys.argv[1]

    server = Master(dynamo_file, master_file, task_server_file, addr)
    s = zerorpc.Server(server)
    s.bind('tcp://' + addr)
    server.start()
    s.run()