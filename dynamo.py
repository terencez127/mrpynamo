"""Implementation of Dynamo

Final iteration: add use of vector clocks for metadata"""
import copy
import random
import logging
import os
import sys
import cPickle as pickle

import logconfig
from node import Node
from timer import TimerManager
from framework import Framework
from hash_multiple import ConsistentHashTable
from dynamomessages import ClientPut, ClientGet, ClientPutRsp, ClientGetRsp
from dynamomessages import PutReq, GetReq, PutRsp, GetRsp
from dynamomessages import DynamoRequestMessage
from dynamomessages import PingReq, PingRsp
from merkle import MerkleTree
from vectorclock import VectorClock
import zerorpc
import leveldb
import gevent

logconfig.init_logging()
_logger = logging.getLogger('dynamo')


# PART dynamonode
class DynamoNode(Node):
    timer_priority = 20
    T = 10  # Number of repeats for nodes in consistent hash table
    N = 3  # Number of nodes to replicate at
    W = 2  # Number of nodes that need to reply to a write operation
    R = 3  # Number of nodes that need to reply to a read operation
    nodelist = []
    chash = ConsistentHashTable(nodelist, T)

    def __init__(self, addr, config_file):
        super(DynamoNode, self).__init__()
        self.local_store = MerkleTree()  # key => (value, metadata)
        self.pending_put_rsp = {}  # seqno => set of nodes that have stored
        self.pending_put_msg = {}  # seqno => original client message
        self.pending_get_rsp = {}  # seqno => set of (node, value, metadata) tuples
        self.pending_get_msg = {}  # seqno => original client message
        # seqno => set of requests sent to other nodes, for each message class
        self.pending_req = {PutReq: {}, GetReq: {}}
        self.failed_nodes = []
        self.pending_handoffs = {}
        
        #modifed
        self.servers = []
        self.num = 3 
        self.addr = addr
        
        if not os.path.exists('./' + addr):
            os.mkdir('./' + addr)
        self.db = leveldb.LevelDB('./' + addr + '/db')
        f = open(config_file, 'r')
        for line in f.readlines():
            line = line.rstrip()
            self.servers.append(line)
        print 'My addr: %s' % (self.addr)
        print 'Server list: %s' % (str(self.servers))

        self.connections = []

        for i, server in enumerate(self.servers):
            DynamoNode.nodelist.append(server)                
            if server == self.addr:
                self.i = i
                self.connections.append(self)
            else:
                c = zerorpc.Client(timeout=10)
                c.connect('tcp://' + server)
                self.connections.append(c)                
                
        if not os.path.exists(addr):
            os.mkdir(addr)
        ###################################
        
        # Rebuild the consistent hash table
        #modified
#        DynamoNode.nodelist.append(self)
#        print "append node: ", self,  len(DynamoNode.nodelist)
###################################
        DynamoNode.chash = ConsistentHashTable(DynamoNode.nodelist, DynamoNode.T)
        # Run a timer to retry failed nodes
        #modified
        self.pool = gevent.pool.Group()
        self.check_servers_greenlet = self.pool.spawn(self.retry_failed_node)
#        self.retry_failed_node("retry")
        #################################


# PART reset
    @classmethod
    def reset(cls):
        cls.nodelist = []
        cls.chash = ConsistentHashTable(cls.nodelist, cls.T)

# PART storage
    def store(self, key, value, metadata):
        self.local_store[key] = (value, metadata)
        #modified
        self.db.Put(key, value)
#        print "sotre:", key, value
        #########################

    def retrieve(self, key):
        try: 
            return self.db.Get(key), None
        except Exception:
            return None, None
#        if key in self.local_store:
#            #modified
#            return self.db.Get(key), self.local_store[key][1]
#            #########################
##            return self.local_store[key]
#        else:
#            return (None, None)

# PART retry_failed_node

    
    def retry_failed_node(self):  # Permanently repeating timer
        #modified
        while True:
        ####################
            gevent.sleep(5)
#            print 'sleeping...'
            if self.failed_nodes:
                
                if len(self.failed_nodes) < 1:
                    continue
#                print "self.failed_nodes: ",self.failed_nodes
                node = self.failed_nodes.pop(0)
                # Send a test message to the oldest failed node
                pingmsg = PingReq(self.addr, node)
                #modified
                con = self.connections[self.servers.index(node)]
                result = Framework.send_message(pingmsg, con)
                if result is False and node not in self.failed_nodes:
                    self.failed_nodes.append(node)
                ############################
        # Restart the timer
        TimerManager.start_timer(self, reason="retry", priority=15, callback=None)#self.retry_failed_node)

    def rcv_pingreq(self, pingmsg):
        # Always reply to a test message
        pingrsp = PingRsp(pingmsg)
#        print '-------------------------------------pingreq', pingmsg.from_node, pingmsg.to_node
        #modified
        con = self.connections[self.servers.index(pingrsp.to_node)]
        result = Framework.send_message(pingrsp, con)
        if result is False and pingrsp.to_node not in self.failed_nodes:
            self.failed_nodes.append(pingrsp.to_node)
        #########################################

    def rcv_pingrsp(self, pingmsg):
        # Remove all instances of recovered node from failed node list
#        print '+++++++++++++++++++++++++++++++++++++pingrsp', pingmsg.from_node, pingmsg.to_node
        recovered_node = pingmsg.from_node
        while recovered_node in self.failed_nodes:
            self.failed_nodes.remove(recovered_node)
#        print recovered_node in self.pending_handoffs
        if recovered_node in self.pending_handoffs:
            for key in self.pending_handoffs[recovered_node]:
                # Send our latest value for this key
                (value, metadata) = self.retrieve(key)
                putmsg = PutReq(self.addr, recovered_node, key, value, metadata)
                
                #modified
                con = self.connections[self.servers.index(putmsg.to_node)]
                result = Framework.send_message(putmsg, con)
#                if result is False:
#                    self.failed_nodes.append(recovered_node)
#                    break
                #######################################
#            print    "==========:",recovered_node
            if recovered_node in self.pending_handoffs:
                del self.pending_handoffs[recovered_node]
            
            

# PART rsp_timer_pop
    def rsp_timer_pop(self, reqmsg):
        # no response to this request; treat the destination node as failed
        _logger.info("Node %s now treating node %s as failed", self, reqmsg.to_node)
        self.failed_nodes.append(reqmsg.to_node)
        failed_requests = Framework.cancel_timers_to(reqmsg.to_node)
        failed_requests.append(reqmsg)
        for failedmsg in failed_requests:
            self.retry_request(failedmsg)

    def retry_request(self, reqmsg):
        if not isinstance(reqmsg, DynamoRequestMessage):
            return
        # Send the request to an additional node by regenerating the preference list
        preference_list = DynamoNode.chash.find_nodes(reqmsg.key, DynamoNode.N, self.failed_nodes)[0]
        kls = reqmsg.__class__
        # Check the pending-request list for this type of request message
        if kls in self.pending_req and reqmsg.msg_id in self.pending_req[kls]:
            for node in preference_list:
                if node not in [req.to_node for req in self.pending_req[kls][reqmsg.msg_id]]:
                    # Found a node on the new preference list that hasn't been sent the request.
                    # Send it a copy
                    newreqmsg = copy.copy(reqmsg)
                    newreqmsg.to_node = node
                    self.pending_req[kls][reqmsg.msg_id].add(newreqmsg)
                    
                    #modified 
                    con = self.connections[self.servers.index(newreqmsg.to_node)]
                    Framework.send_message(newreqmsg, con)
                    ###########################
                    
# PART rcv_clientput
    def rcv_clientput(self, msg):
        preference_list, avoided = DynamoNode.chash.find_nodes(msg.key, DynamoNode.N, self.failed_nodes)
        non_extra_count = DynamoNode.N - len(avoided)
        # Determine if we are in the list
        #modified
        if self.addr not in preference_list:
            # Forward to the coordinator for this key
            _logger.info("put(%s=%s) maps to %s", msg.key, msg.value, preference_list)
            result = True
            for e in preference_list:
                con = self.connections[self.servers.index(e)]
                result = Framework.forward_message(msg, con, e)
                if result is not False:
                    break
                if e not in self.failed_nodes:
                    self.failed_nodes.append(e)
            return result 
        #####################################
        
        else:
            # Use an incrementing local sequence number to distinguish
            # multiple requests for the same key
            seqno = self.generate_sequence_number()
            _logger.info("%s, %d: put %s=%s", self, seqno, msg.key, msg.value)
            # The metadata for a key is passed in by the client, and updated by the coordinator node.
            metadata = copy.deepcopy(msg.metadata)
            metadata.update(self.name, seqno)
            # Send out to preference list, and keep track of who has replied
            self.pending_req[PutReq][seqno] = set()
            self.pending_put_rsp[seqno] = set()
            self.pending_put_msg[seqno] = msg
            reqcount = 0
            
            #modified
            nodes = []
            #####################
            for ii, node in enumerate(preference_list):
                if ii >= non_extra_count:
                    # This is an extra node that's only include because of a failed node
                    handoff = avoided
                else:
                    handoff = None
                # Send message to get node in preference list to store
                putmsg = PutReq(self.addr, node, msg.key, msg.value, metadata, msg_id=seqno, handoff=handoff)
                self.pending_req[PutReq][seqno].add(putmsg)

                #modified
                con = self.connections[self.servers.index(putmsg.to_node)]
                result = Framework.send_message(putmsg, con)
                if result is False and putmsg.to_node not in self.failed_nodes:
                    self.failed_nodes.append(putmsg.to_node)
                if result is not False:
                    nodes.append(node)
                    reqcount = reqcount + 1
                ####################################
#                print "---------------------------------", type(putmsg)                
                if reqcount >= DynamoNode.N:
                    # preference_list may have more than N entries to allow for failed nodes
                    break
            #modified
            if reqcount >= DynamoNode.N:
                return nodes
            return False
            ###############
    
# PART rcv_clientget
    def rcv_clientget(self, msg):
        preference_list = DynamoNode.chash.find_nodes(msg.key, DynamoNode.N, self.failed_nodes)[0]
        # Determine if we are in the list
        
        #modified
        if self.addr not in preference_list:
        ################################
            # Forward to the coordinator for this key
            _logger.info("get(%s=?) maps to %s", msg.key, preference_list)
            for e in preference_list:
                con = self.connections[self.servers.index(e)]
                result = Framework.forward_message(msg, con, e)
                if result is not False:
                    break
                if e not in self.failed_nodes:
                    self.failed_nodes.append(e)
            return result
        else:
            seqno = self.generate_sequence_number()
            self.pending_req[GetReq][seqno] = set()
            self.pending_get_rsp[seqno] = set()
            self.pending_get_msg[seqno] = msg
            reqcount = 0
            #modified
            value = []
            ################
            for node in preference_list:
                getmsg = GetReq(self.addr, node, msg.key, msg_id=seqno)
                self.pending_req[GetReq][seqno].add(getmsg)               
                #modified
                con = self.connections[self.servers.index(getmsg.to_node)]
                result = Framework.send_message(getmsg, con)
                if result is not False:
                    value.append(result)                           
                    reqcount = reqcount + 1
                ############################################
                if reqcount >= DynamoNode.N:
                    # preference_list may have more than N entries to allow for failed nodes
                    break
#            print value
            #modified
            # no value for this key
            if len(value) < 1:
                return False
            ##########################
            return value

# PART rcv_put
    def rcv_put(self, putmsg):
        _logger.info("%s: store %s=%s on %s", self, putmsg.key, putmsg.value, self.addr)
        self.store(putmsg.key, putmsg.value, putmsg.metadata)
        if putmsg.handoff is not None:
            for failed_node in putmsg.handoff:
                if failed_node not in self.failed_nodes:
                    self.failed_nodes.append(failed_node)
                if failed_node not in self.pending_handoffs:
                    self.pending_handoffs[failed_node] = set()
                self.pending_handoffs[failed_node].add(putmsg.key)
        putrsp = PutRsp(putmsg)
        
        #modified
        return 
#        con = self.connections[self.servers.index(putrsp.to_node)]
#        Framework.send_message(putrsp, con)
        #################################

# PART rcv_putrsp
    def rcv_putrsp(self, putrsp):
        seqno = putrsp.msg_id
        if seqno in self.pending_put_rsp:
            self.pending_put_rsp[seqno].add(putrsp.from_node)
            if len(self.pending_put_rsp[seqno]) >= DynamoNode.W:
                _logger.info("%s: written %d copies of %s=%s so done", self, DynamoNode.W, putrsp.key, putrsp.value)
#                _logger.debug("  copies at %s", [node.name for node in self.pending_put_rsp[seqno]])
                # Tidy up tracking data structures
                original_msg = self.pending_put_msg[seqno]
                del self.pending_req[PutReq][seqno]
                del self.pending_put_rsp[seqno]
                del self.pending_put_msg[seqno]
                # Reply to the original client
                client_putrsp = ClientPutRsp(original_msg, putrsp.metadata)
                
                #mofied
                con = self.connections[self.servers.index(client_putrsp.to_node)]
                Framework.send_message(client_putrsp, con)
                ###################################
        else:
            pass  # Superfluous reply

# PART rcv_get
    def rcv_get(self, getmsg):
        _logger.info("%s: retrieve %s=?", self, getmsg.key)
        (value, metadata) = self.retrieve(getmsg.key)
        getrsp = GetRsp(getmsg, value, metadata)
        
        #modified
#        print "rcv_get", value
        return value
#        con = self.connections[self.servers.index(getrsp.to_node)]
#        Framework.send_message(getrsp, con)
        #############################33
        
# PART rcv_getrsp
    def rcv_getrsp(self, getrsp):
        seqno = getrsp.msg_id
        if seqno in self.pending_get_rsp:
            self.pending_get_rsp[seqno].add((getrsp.from_node, getrsp.value, getrsp.metadata))
            if len(self.pending_get_rsp[seqno]) >= DynamoNode.R:
                _logger.info("%s: read %d copies of %s=? so done", self, DynamoNode.R, getrsp.key)
#                _logger.debug("  copies at %s", [(node.name, value) for (node, value, _) in self.pending_get_rsp[seqno]])
                # Coalesce all compatible (value, metadata) pairs across the responses
                results = VectorClock.coalesce2([(value, metadata) for (node, value, metadata) in self.pending_get_rsp[seqno]])
                # Tidy up tracking data structures
                original_msg = self.pending_get_msg[seqno]
                del self.pending_req[GetReq][seqno]
                del self.pending_get_rsp[seqno]
                del self.pending_get_msg[seqno]
                # Reply to the original client, including all received values
                client_getrsp = ClientGetRsp(original_msg,
                                             [value for (value, metadata) in results],
                                             [metadata for (value, metadata) in results])
                #modified
                con = self.connections[self.servers.index(client_getrsp.to_node)]
                Framework.send_message(client_getrsp, con)
                ########################################
                
        else:
            pass  # Superfluous reply

# PART rcvmsg
    def rcvmsg(self, pmsg):
        
        #modified
        msg = pickle.loads(pmsg)
        
        #########################
        
        if isinstance(msg, ClientPut):
#            print "reveive message", msg
            return self.rcv_clientput(msg)
        elif isinstance(msg, PutReq):
            return self.rcv_put(msg)
        elif isinstance(msg, PutRsp):
            self.rcv_putrsp(msg)
        elif isinstance(msg, ClientGet):
            return self.rcv_clientget(msg)
        elif isinstance(msg, GetReq):
            return self.rcv_get(msg)
        elif isinstance(msg, GetRsp):
            self.rcv_getrsp(msg)
        elif isinstance(msg, PingReq):
            self.rcv_pingreq(msg)
        elif isinstance(msg, PingRsp):
            self.rcv_pingrsp(msg)
        else:
            raise TypeError("Unexpected message type %s", msg.__class__)

# PART get_contents
    def get_contents(self):
        results = []
        for key, value in self.local_store.items():
            results.append("%s:%s" % (key, value[0]))
        return results


# PART clientnode
class DynamoClientNode(Node):
    timer_priority = 17

    def __init__(self, addr, config_file, name=None):
        super(DynamoClientNode, self).__init__(name)
        self.last_msg = None  # Track last received message
        
        self.servers = []
        f = open(config_file, 'r')
        for line in f.readlines():
            line = line.rstrip()
            self.servers.append(line)
        
        #modified
        self.addr = addr
        self.connections = []
        for server in self.servers:   
            DynamoNode.nodelist.append(server)             
            c = zerorpc.Client(timeout=10)
            c.connect('tcp://' + server)
            self.connections.append(c)

    def put(self, key, metadata, value, destnode=None):
#        print "node: ",len(DynamoNode.nodelist)
        #modified
        temp = metadata
        while True:
        ###################################
            metadata = temp
            if destnode is None:  # Pick a random node to send the request to
                destnode = random.choice(DynamoNode.nodelist)
            # Input metadata is always a sequence, but we always need to insert a
            # single VectorClock object into the ClientPut message
#            print '-------------------------choice:', destnode
            if len(metadata) == 1 and metadata[0] is None:
                metadata = VectorClock()
            else:
                # A Put operation always implies convergence
                metadata = VectorClock.converge(metadata)
            putmsg = ClientPut(self.addr, destnode, key, value, metadata)
            
            #modified
            con  = self.connections[self.servers.index(destnode)]
            result = Framework.send_message(putmsg, con)
            if result is not False:
                break
            destnode = None
        ##################################
        return result

    def get(self, key, destnode=None):
        #modified
        while True:
        ###################################
            if destnode is None:  # Pick a random node to send the request to
                destnode = random.choice(DynamoNode.nodelist)
            getmsg = ClientGet(self.addr, destnode, key)
            #modified
            con  = self.connections[self.servers.index(destnode)]
            result = Framework.send_message(getmsg, con)
            if result is not False:
                return result
#        return Framework.send_message(getmsg, con)

#        return getmsg
        ##################################

    def rsp_timer_pop(self, reqmsg):
        if isinstance(reqmsg, ClientPut):  # retry
            _logger.info("Put request timed out; retrying")
            self.put(reqmsg.key, [reqmsg.metadata], reqmsg.value)
        elif isinstance(reqmsg, ClientGet):  # retry
            _logger.info("Get request timed out; retrying")
            self.get(reqmsg.key)

# PART clientrcvmsg
    def rcvmsg(self, msg):
        self.last_msg = msg
        
if __name__ == '__main__':
    addr = sys.argv[1]
    d = DynamoNode(addr, "addr.cfg")
    server = zerorpc.Server(d)
    server.bind('tcp://' + addr)
    print '[%s] Starting Dynamo Server' % addr
#    d.start()
    server.run()