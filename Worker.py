class WorkerBase():
    def __init__(self, master, task_id):
        self.task_server = task_server
        self.master = master
        pass

    def execute(self, ):
        pass

    def output(self):
        pass


class Mapper(WorkerBase):

    pass


class Reducer(WorkerBase):

    def on_receive_data(self):
        pass

