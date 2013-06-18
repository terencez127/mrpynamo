import logging
import worker
from collections import Counter
import cPickle as pickle


class MyMapper(worker.Mapper):
    def map(self, data):
        print 'map()'
        c = Counter([n.strip().lower() for n in data.split()])
        ret = [pickle.dumps(c)]
        return ret


class MyReducer(worker.Reducer):
    def __init__(self, dynamo):
        worker.Reducer.__init__(self, dynamo)
        self.counter = None

    def reduce(self, data):
        print 'reduce()'
        new_coutner = pickle.loads(data[0])
        if self.counter is None:
            self.counter = new_coutner
        else:
            self.counter += new_coutner

    def output(self):
        ret = ''
        for i in self.counter.items():
            ret += str(i[0]) +': ' + str(i[1]) + '\n'
        return ret