
class Mapper():
    def __init__(self, dynamo_client):
        self.dynamo_client = dynamo_client

    def map(self, data):
        raise NotImplementedError("Should have implemented this")


class Reducer():
    def __init__(self, dynamo_client):
        self.dynamo_client = dynamo_client

    def reduce(self, data):
        raise NotImplementedError("Should have implemented this")

    def output(self):
        raise NotImplementedError("Should have implemented this")