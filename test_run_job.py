import sys
import zerorpc

from dynamo import DynamoClientNode

slices = 2

if __name__ == '__main__':
    addr = sys.argv[1]
    client = DynamoClientNode(addr, "config_dynamo")



    fname = 'wordcount.py'
    with open(fname, 'r') as f:
        content = f.read()
    client.put(fname, [], content)


    key_list = []

    fname = 'test_word/test_short'
    with open(fname, 'r') as f:
        content = f.read()
    length = len(content)/slices
    for i in xrange(slices):
        key_list.append(('short' + str(i), client.put('short' + str(i),
            [], content[i*length:(i+1)*length])))
        # print client.get('short' + str(i))[0]

    # for i in xrange(slices):
    #     print 'short' + str(i), client.get('short' + str(i))[0]


    with open('config_master', 'r') as f:
        master_list = [i.strip() for i in f.readlines()]

    connections = []
    for i, server in enumerate(master_list):
        c = zerorpc.Client(timeout=10)
        c.connect('tcp://' + server)
        connections.append(c)

    # print key_list
    # key_list = ['short' + str(i) for i in xrange(slices)]
    output_dir = 'short_output'
    src = 'wordcount.py'
    name = 'test_short'
    ret = connections[0].new_job(name, key_list, output_dir, src)
    if ret:
        connections[0].run_job(name)
