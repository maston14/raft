import threading
import time
import random
from SimpleXMLRPCServer import SimpleXMLRPCServer
import xmlrpclib

class State:
    LEADER = 2
    CANDIDATE = 1
    FOLLOWER = 0

class Datacenter:
    def __init__(self, host_id=None, port=None, nodes=None, addresses=None, tickets_left=-1):
        # intially, every server is a follower
        self.host_id = host_id
        self.state = State.FOLLOWER
        self.current_term = -1
        self.leader_id = None
        self.voted_in = -1
        self.port = port
        self.last_update = time.time()
        self.election_timeout = random.uniform(3.0, 5.0)
        self.nodes = nodes
        self.log = []
        self.running_as_leader = False

        # Candidate use
        self.granted_votes = 0
        self.granted_votes_mutex = threading.Lock()

        # Leader use
        self.addresses = addresses
        self.num_of_nodes = len(nodes)
        self.connected_nodes = [False] * self.num_of_nodes
        self.commit_votes_for_current_term = 0
        self.commited_for_current_term = False


        # Application use
        self.tickets_left = tickets_left

        print "[HOST][%s] Datacenter start" % (host_id)

        threading.Thread(target=self.membership, args=()).start()

        print "Raft start"
        print addresses

    # membership
    def membership(self):
        while True:
            while self.state == State.FOLLOWER:
                if self.check_election_timeout():
                    print 'Timeout, I am gonna be the second DT'
                    self.change_state(State.CANDIDATE)

            while self.state == State.CANDIDATE:
                self.run_election()

            while self.state == State.LEADER:
                self.serve_as_leader()



    def run_election(self):
        self.current_term += 1
        print 'Candidate incremented term ' + str(self.current_term)
        self.voted_in = self.current_term
        # TODO: maybe need mutex
        self.granted_votes = 1
        self.reset_election_timeout()
        for (k, v) in addresses.items():
            threading.Thread(target=self.send_a_request_vote_rpc, args=(v[0], v[1])).start()

        while (not self.check_election_timeout()) and (self.granted_votes < (len(addresses) + 1) / 2 + 1):
            continue

        if self.granted_votes >= (len(addresses) + 1) / 2 + 1:
            self.change_state(State.LEADER)
        else:
            self.change_state(State.FOLLOWER)


    def serve_as_leader(self):
        time.sleep(0.5)
        for (k, v) in addresses.items():
            threading.Thread(target=self.send_a_heartbeat, args=(v[0], v[1]))


    # main RPCs
    def request_vote_rpc(self, candidate_id, candidate_term, last_log_index, last_log_term):
        print 'I have been called to vote for node %d in term %d' % candidate_id, candidate_term
        if candidate_term > self.current_term and candidate_term > self.voted_in:
            self.current_term = candidate_term
            self.voted_in = candidate_term
            self.state = State.FOLLOWER
            return self.current_term, True
        return self.current_term, False


    def append_entries_rpc(self, leader_term, leader_id, prev_log_index, prev_log_term, commited_index, entries = []):
        if len(entries) == 0:
            print "got a heartbeat"
            self.reset_election_timeout()
        if leader_term < self.current_term:
            return (self.current_term, False)


    # Utility
    def check_election_timeout(self):
        # True if timeout
        return (self.last_update + self.election_timeout) < time.time()

    def reset_election_timeout(self):
        self.last_update = time.time()

    def send_a_heartbeat(self, address, port):
        url = 'http://' + str(address) + ':' + str(port)
        try:
            s = xmlrpclib.ServerProxy(url)
            # TODO: -1 should be changed
            s.append_entries_rpc(self.current_term, self.host_id, -1, -1, -1)
        except Exception as e:
            print "could not connect.. Exception: " + str(e)

    def send_a_request_vote_rpc(self, address, port):
        url = 'http://' + str(address) + ':' + str(port)
        print 'the url to request a rpc is %s' % url
        success = False
        while (not self.check_election_timeout()) and (not success) and self.state == State.CANDIDATE:
            try:
                s = xmlrpclib.ServerProxy(url)
                term_return, vote_granted = s.request_vote_rpc(self.host_id, self.current_term, len(self.log), -1)
                success = True
                if vote_granted:
                    self.granted_votes_mutex.acquire()
                    self.granted_votes += 1
                    self.granted_votes_mutex.release()
            except Exception as e:
                print "could not connect %s.. Exception: %s" % str(e), str(port)
                time.sleep(0.5)


    def change_state(self, state):
        print "Changed state to " + str(state) + " in term: " + str(self.current_term)
        self.commited_for_current_term = False
        self.commit_votes_for_current_term = 0
        self.state = state

##############################################################################################


# read the config file
def cluster_init(config_filename, host_id):
    # value to return

    # list of other nodes
    nodes = []

    # map of node_id -> ( ip, port )
    addresses = {}

    my_port = None

    # get ticket number
    fin = open(config_filename)
    tickets_left = int(fin.readline().strip())

    # get nodes config
    for aline in fin:
        split_line = aline.strip().split()

        node_id = int(split_line[0])
        host = split_line[1]
        port = int(split_line[2])

        if node_id == host_id:
            my_port = port
            continue

        nodes.append(node_id)
        addresses[node_id] = (str(host), port)

    return my_port, nodes, addresses, tickets_left


if __name__ == '__main__':
    import sys

    host_id = int(sys.argv[1])
    config_filename = sys.argv[2]

    my_port, nodes, addresses, tickets_left = cluster_init(config_filename = config_filename, host_id = host_id)
    datacenter_obj = Datacenter(host_id, my_port, nodes, addresses, tickets_left)

    server = SimpleXMLRPCServer(("localhost", my_port), allow_none=True)
    print "Listening on port %d..." % my_port
    server.register_multicall_functions()
    server.register_instance(datacenter_obj)
    server.serve_forever()