import threading
import time
import random
from SimpleXMLRPCServer import SimpleXMLRPCServer
from SocketServer import ThreadingMixIn
import xmlrpclib
import sys

# constant def
LEADER = "LEADER"
CANDIDATE = "CANDIDATE"
FOLLOWER = "FOLLOWER"
TIMEOUT_LOW = 3.0
TIMEOUT_HIGH = 6.0
HEARTBEAT_INTERVAL = 1.0
MESSAGE_DELAY = 0.0

class Entry(object):
    def __init__(self, term=-1, command="", string=None):
        super(Entry, self).__init__()
        if string != None:
            split_line = string.split("|", 1)
            self.term = int(split_line[0])
            self.command = split_line[1]
        else:
            self.term = term
            self.command = command

    def to_string(self):
        return str(self.term) + "|" + str(self.command)

# datacenter
class Datacenter(object):
    def __init__(self, host_id=None, ip=None, port=None, addresses=None, tickets_left=-1):

        super(Datacenter, self).__init__()
        # self connection
        self.host_id = host_id
        self.ip = None
        self.port = port

        # global config
        self.addresses = addresses  # {id: (ip, port)}

        # local state
        self.state = FOLLOWER
        self.current_term = 0
        self.leader_id = None
        self.voted_in = 0
        self.log = []
        self.last_committed_index = -1

        self.last_update = time.time()
        self.election_timeout = random.uniform(TIMEOUT_LOW, TIMEOUT_HIGH)

        # Candidate use
        self.granted_votes = 0
        self.granted_votes_mutex = threading.Lock()

        # Leader use
        self.commit_votes_for_current_term = 0
        self.commited_for_current_term = False

        # Application use
        self.tickets_left = tickets_left

    def start(self):
        print "[HOST][%s] Datacenter start" % (self.host_id)
        print self.addresses
        threading.Thread(target=self.membership, args=()).start()
        print "Raft start with timeout: %f" % self.election_timeout

    # membership
    def membership(self):
        while True:
            while self.state == FOLLOWER:
                if self.check_election_timeout():
                    print 'Timeout, I am gonna be the second DT'
                    self.change_state(CANDIDATE)

            while self.state == CANDIDATE:
                self.run_election()

            while self.state == LEADER:
                self.serve_as_leader()
                time.sleep(HEARTBEAT_INTERVAL)

    def run_election(self):
        self.current_term += 1
        print 'Candidate incremented term ' + str(self.current_term)
        self.voted_in = self.current_term
        # TODO: maybe need mutex
        self.granted_votes = 1
        self.reset_election_timeout()
        for node_id in self.addresses:
            threading.Thread(target=self.send_a_request_vote_rpc, args=self.addresses[node_id]).start()

        while (self.state == CANDIDATE) and (not self.check_election_timeout()) and (
            self.granted_votes < (len(self.addresses) + 1) / 2 + 1):
            continue

        if self.granted_votes >= (len(self.addresses) + 1) / 2 + 1:
            self.change_state(LEADER)
            self.leader_id = self.host_id
        else:
            self.change_state(FOLLOWER)

    def serve_as_leader(self):
        for node_id in self.addresses:
            threading.Thread(target=self.send_a_heartbeat, args=self.addresses[node_id]).start()

    # main RPCs
    def request_vote_rpc(self, candidate_id, candidate_term, last_log_index, last_log_term):
        time.sleep(MESSAGE_DELAY)

        # when candidate have higher term than I have and I have not vote in that candidate term, I vote
        if candidate_term > self.current_term and candidate_term > self.voted_in:
            self.current_term = candidate_term
            self.voted_in = candidate_term
            self.step_down()
            self.reset_election_timeout()
            print 'grant vote for candidate %d in term %d' % (candidate_id, candidate_term)
            time.sleep(MESSAGE_DELAY)
            return self.current_term, True
        else:
            print 'reject vote for candidate %d in term %d' % (candidate_id, candidate_term)
            time.sleep(MESSAGE_DELAY)
            return self.current_term, False

    def append_entries_rpc(self, leader_term, leader_id, prev_log_index, prev_log_term, commited_index, entries):
        time.sleep(MESSAGE_DELAY)

        entries = [Entry(-1, "", string) for string in entries]

        print "receive append from node[%d] in term %d" % (leader_id, leader_term)
        if leader_term < self.current_term:
            time.sleep(MESSAGE_DELAY)
            return (self.current_term, False)

        # update leader info
        self.current_term = leader_term
        self.step_down()
        self.leader_id = leader_id
        self.reset_election_timeout()

        # last term not match
        if len(self.log) > 0 and (prev_log_index > len(self.log) - 1 or prev_log_term != self.log[prev_log_index].term):
            time.sleep(MESSAGE_DELAY)
            return (self.current_term, False)
        if len(self.log) <= 0 and prev_log_index != -1:
            time.sleep(MESSAGE_DELAY)
            return (self.current_term, False)

        # append new entries anyway
        if len(entries) > 0:
            self.log = self.log[:prev_log_index + 1] + entries

        # commit
        for i in range(self.last_committed_index + 1, commited_index + 1):
            self.commit_entry(i)
            self.last_committed_index = i

        time.sleep(MESSAGE_DELAY)
        return (self.current_term, True)

    # Utility
    def commit_entry(self, index):
        # TODO
        pass

    def check_election_timeout(self):
        # True if timeout
        return (self.last_update + self.election_timeout) < time.time()

    def reset_election_timeout(self):
        self.last_update = time.time()

    def step_down(self):
        self.state = FOLLOWER

    def send_a_heartbeat(self, ip, port, index=0):
        url = 'http://' + str(ip) + ':' + str(port)
        try:
            s = xmlrpclib.ServerProxy(url)
            prev_log_index = index - 1
            if len(self.log) > prev_log_index and prev_log_index >= 0:
                prev_log_term = self.log[prev_log_index].term
            else:
                prev_log_term = -1
            commited_index = -1
            success = False
            # repair all entries
            while not success:
                target_term, success = s.append_entries_rpc(self.current_term, self.host_id, prev_log_index,
                                                            prev_log_term, commited_index,
                                                            [en.to_string() for en in self.log[index:]])
                prev_log_index -= 1
                if len(self.log) > prev_log_index and prev_log_index >= 0:
                    prev_log_term = self.log[prev_log_index].term
                else:
                    prev_log_term = -1

        except Exception as e:
            print "could not send heartbeat to %s.. Exception: %s" % (str(port), str(e))

    def send_a_request_vote_rpc(self, ip, port):
        url = 'http://' + str(ip) + ':' + str(port)
        success = False
        if len(self.log) > 0:
            term = self.log[-1].term
        else:
            term = -1

        while (not self.check_election_timeout()) and (not success) and self.state == CANDIDATE:
            try:
                s = xmlrpclib.ServerProxy(url)
                term_return, vote_granted = s.request_vote_rpc(self.host_id, self.current_term, len(self.log) - 1, term)
                success = True
                if vote_granted:
                    self.granted_votes_mutex.acquire()
                    self.granted_votes += 1
                    self.granted_votes_mutex.release()
            except Exception as e:
                print "could not connect %s.. Exception: %s" % (str(port), str(e))
                time.sleep(0.5)

    def change_state(self, state):
        self.commited_for_current_term = False
        self.commit_votes_for_current_term = 0
        self.state = state
        print "Changed state to " + str(state) + " in term: " + str(self.current_term)

    def get_a_rpc_connection_to_leader(self):
        ip = self.addresses[self.leader_id][0]
        port = self.addresses[self.leader_id][1]
        url = 'http://' + str(ip) + ':' + str(port)
        try:
            s = xmlrpclib.ServerProxy(url)
            return s
        except Exception as e:
            print "could not reach leader at %s.. Exception: %s" % (str(port), str(e))

    # Client rpc

    def buy_ticket_rpc(self, num):
        # if I am the leader
        if self.host_id == self.leader_id:
            # a new entry
            entry = Entry(self.current_term, num)
            self.log.append(entry)

            # then try to get it commit
        else:
            # forward it to the leader
            rpc_connection = self.get_a_rpc_connection_to_leader()
            try:
                rpc_connection.buy_ticket_rpc(num)
            except Exception as e:
                print "could not reach leader .. Exception: %s" % str(e)


class SimpleThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass

##############################################################################################
# read the config file and start a datacenter
def cluster_init(config_filename, host_id):
    # map of node_id -> ( ip, port )
    addresses = {}

    # get ticket number
    fin = open(config_filename)
    tickets_left = int(fin.readline().strip())

    # get nodes config
    for aline in fin:
        split_line = aline.strip().split()

        node_id = int(split_line[0])
        ip = split_line[1]
        port = int(split_line[2])

        if node_id == host_id:
            datacenter_obj = Datacenter(host_id, ip, port, addresses, tickets_left)
            continue

        addresses[node_id] = (str(ip), port)

    return datacenter_obj


if __name__ == '__main__':
    host_id = int(sys.argv[1])
    config_filename = sys.argv[2]

    datacenter_obj = cluster_init(config_filename=config_filename, host_id=host_id)
    datacenter_obj.start()

    server = SimpleThreadedXMLRPCServer(("localhost", datacenter_obj.port), allow_none=True, logRequests=False)
    print "Listening on port %d..." % datacenter_obj.port
    server.register_multicall_functions()
    server.register_instance(datacenter_obj)
    server.serve_forever()

