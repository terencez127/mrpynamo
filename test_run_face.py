import sys
import zerorpc

from dynamo import DynamoClientNode

slices = 2

if __name__ == '__main__':
    addr = sys.argv[1]
    client = DynamoClientNode(addr, "config_dynamo")

    fname = 'face_recognization.py'
    with open(fname, 'r') as f:
        content = f.read()
    client.put(fname, [], content)

    key_list = []

    for i in xrange(1, 6):
        print i
        with open('test_face/'+str(i)+'.jpg', 'r') as f:
            content = f.read()
        key_list.append((str(i)+'.jpg',
                         client.put(str(i)+'.jpg', [], content)))
    with open('test_face/sample.jpg', 'r') as f:
        content = f.read()
    client.put('sample.jpg', [], content)

    with open('config_master', 'r') as f:
        master_list = [i.strip() for i in f.readlines()]

    connections = []
    for i, server in enumerate(master_list):
        c = zerorpc.Client(timeout=10)
        c.connect('tcp://' + server)
        connections.append(c)

    # print key_list
    # key_list = ['short' + str(i) for i in xrange(slices)]
    output_dir = 'face_output'
    src = 'face_recognization.py'
    name = 'test_face'
    ret = connections[0].new_job(name, key_list, output_dir, src)
    if ret:
        connections[0].run_job(name)
