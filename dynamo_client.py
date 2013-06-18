import sys

from dynamo import DynamoClientNode
import cPickle as pickle

slices = 2

if __name__ == '__main__':
    addr = sys.argv[1]
    client = DynamoClientNode(addr, "config_dynamo")

    fname = 'face_output'
    with open(fname, 'w+') as f:
        result = client.get(fname)[0]
        print 'len:', len(result)
        n = pickle.loads(result)

        print 'type ', type(n)
        for i, data in enumerate(n):
            with open('test_face/output/' + str(i) + '_output_' + '.jpg', 'w+') as f:
                f.write(data)
        # f.write(result)

    # fname = 'face_recognization.py'
    # with open(fname, 'r') as f:
    #     content = f.read()
    # client.put(fname, [], content)
    #
    # # fname = 'test_face/test_short'
    #
    # # length = len(content)/slices
    # for i in xrange(1, 4):
    #     with open('test_face/'+str(i)+'.jpg', 'r') as f:
    #         content = f.read()
    #     client.put('test_face/'+str(i)+'.jpg', [], content)
    # with open('sample.jpg', 'r') as f:
    #     content = f.read()
    # client.put('sample.jpg', [], content)
    #     # client.put('face' + str(i), [], content[i*length:(i+1)*length])
    #     # print client.get('face' + str(i))[0]
    # #
    # # for i in xrange(slices):
    # #     print 'short' + str(i), client.get('short' + str(i))[0]
