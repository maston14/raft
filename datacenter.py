import threading
import time
import random
from SimpleXMLRPCServer import SimpleXMLRPCServer
from SocketServer import ThreadingMixIn
import xmlrpclib
import sys
import collections
import traceback
import os

# constant def
LEADER = "LEADER"
CANDIDATE = "CANDIDATE"
FOLLOWER = "FOLLOWER"
TIMEOUT_LOW = 3.0
TIMEOUT_HIGH = 6.0
HEARTBEAT_INTERVAL = 1.0
MESSAGE_DELAY = 0.0
KILL = False

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

class Datacenter(object):
    def __init__(self, host_id=None, ip=None, port=None, addresses=None, tickets_left=-1):

        super(Datacenter, self).__init__()
        # self connection
        self.host_id = host_id
        self.ip = ip
        self.port = port

        # global config
        self.addresses = addresses  # {id: (ip, port)}
        self.old_addresses = {}

        # local state
        self.state = FOLLOWER
        self.current_term = 0
        self.leader_id = None
        self.voted_in = 0
        self.log = []
        self.log_mutex = threading.Lock()
        # record the commited enrty and its result, mapped one by one with log; True means this entry executed by state machine and return true
        self.committed_entry_result = []
        self.last_committed_index = -1

        self.last_update = time.time()
        self.election_timeout = random.uniform(TIMEOUT_LOW, TIMEOUT_HIGH)

        # Candidate use
        self.granted_votes = set()
        self.granted_votes_mutex = threading.Lock()

        # Leader use
        # record each follower's next index
        self.followers_next_index = {}
        # initialize
        for node_id in self.addresses:
            if node_id != self.host_id:
                self.followers_next_index[node_id] = 0

        # used for record followes' voting status for each entry. if it has majority then commit;
        # new leader must commit one entry of its own term then commit the possible previous term
        self.index_append_count = collections.defaultdict(set) # {index: count}
        self.commit_mutex = threading.Lock()

        # Application use
        self.tickets_left = tickets_left

    def start(self):
        print "[HOST][%s] Datacenter start" % (self.host_id)
        print "Under Config: ", self.addresses

        threading.Thread(target=self.membership, args=()).start()

        print "Initial timeout: %f" % self.election_timeout

        threading.Thread(target=self.check_commit, args=()).start()

    # membership: keep running to check the membership
    def membership(self):
        while True:
            if KILL:
                return

            if self.state == FOLLOWER:
                if self.check_election_timeout():
                    print '[%s] Timeout, I am gonna be the second Donald to run an election' % time.time()
                    self.change_state(CANDIDATE)

            if self.state == CANDIDATE:
                self.run_election()

            if self.state == LEADER:
                self.serve_as_leader()
                time.sleep(HEARTBEAT_INTERVAL)

    def run_election(self):
        if self.host_id not in self.addresses:
            print "not in service anymore"
            os._exit(0)
        self.current_term += 1
        print 'Candidate incremented term ' + str(self.current_term)
        self.voted_in = self.current_term

        self.granted_votes = set()
        self.reset_election_timeout()
        # send request_vote to everyone.
        for node_id in set(self.addresses) | set(self.old_addresses):
            if node_id != self.host_id:
                threading.Thread(target=self.send_a_request_vote_rpc, args=(node_id,) ).start()

        win = self.check_majority(self.granted_votes)
        # keep waiting until knowing a leader or timeout or not win
        while (self.state == CANDIDATE) and (not self.check_election_timeout()) and (not win):
            win = self.check_majority(self.granted_votes)
            if KILL:
                return

        if win:
            self.change_state(LEADER)
            self.leader_id = self.host_id
            # update followers' next index to leader's log's last index + 1
            for node_id in self.addresses:
                if node_id != self.host_id:
                    self.followers_next_index[node_id] = len(self.log)
        else:
            self.change_state(FOLLOWER)

    def serve_as_leader(self):
        for node_id in set(self.addresses) | set(self.old_addresses):
            if node_id != self.host_id:
                next_index = self.followers_next_index[node_id]
                # send out heartbeat
                threading.Thread(target=self.send_an_append_entries, args=(node_id, next_index)).start()



    # main RPCs
    def request_vote_rpc(self, candidate_id, candidate_term, last_log_index, last_log_term):
        time.sleep(MESSAGE_DELAY)

        # when candidate have higher term than I have and I have not vote in that candidate term, I vote on certain conditions
        if candidate_term > self.current_term and candidate_term > self.voted_in:
            if len(self.log) == 0 or \
                    (len(self.log) > 0 and ( last_log_term > self.log[-1].term or (last_log_term == self.log[-1].term and last_log_index >= len(self.log) - 1))):

                self.current_term = candidate_term
                self.voted_in = candidate_term
                self.reset_election_timeout()
                self.change_state(FOLLOWER)
                print '[%s] grant vote for candidate %d in term %d' % (time.time(), candidate_id, candidate_term)
                time.sleep(MESSAGE_DELAY)
                return self.current_term, True

            print 'reject vote for candidate %d in term %d' % (candidate_id, candidate_term)
            time.sleep(MESSAGE_DELAY)
            return self.current_term, False


        else:
            print 'reject vote for candidate %d in term %d' % (candidate_id, candidate_term)
            time.sleep(MESSAGE_DELAY)
            return self.current_term, False

    def append_entries_rpc(self, leader_term, leader_id, prev_log_index, prev_log_term, commited_index, entries):
        time.sleep(MESSAGE_DELAY)

        if len(entries) == 0:
            #print "receive heartbeat from node[%d] in term %d" % (leader_id, leader_term)
            #print 'heartbeat params: ', prev_log_index, prev_log_term, commited_index, entries
            pass
        else:
            print "receive append from node[%d] in term %d for entries %s" % (leader_id, leader_term, entries)
            #print 'params: ' , prev_log_index, prev_log_term, commited_index, entries
            pass

        entries = [Entry(-1, "", string) for string in entries]

        if leader_term < self.current_term:
            time.sleep(MESSAGE_DELAY)
            print "F1"
            return (self.current_term, False)

        # update leader info
        self.current_term = leader_term
        if self.state != FOLLOWER:
            self.change_state(FOLLOWER)
        self.leader_id = leader_id
        self.reset_election_timeout()

        # last term not match
        if len(self.log) > 0 and (prev_log_index > len(self.log) - 1 or prev_log_term != self.log[prev_log_index].term):
            time.sleep(MESSAGE_DELAY)
            print "F2"
            log = [Entry(-1, "", string) for string in self.log]
            print "******log is: %s" % log

            return (self.current_term, False)

        if len(self.log) <= 0 and prev_log_index != -1:
            time.sleep(MESSAGE_DELAY)
            print "F3"
            return (self.current_term, False)

        # append new entries anyway
        if len(entries) > 0:
            self.append_entries(entries, prev_log_index)

        # commit
        for i in range(len(self.committed_entry_result) - 1 + 1, commited_index + 1):
            self.commit_entry()
        time.sleep(MESSAGE_DELAY)
        #print "T"
        return (self.current_term, True)

    # Utility

    # check whether a set has a size that satisfy majority
    def check_majority(self, id_set):
        new_count = len(id_set & set(self.addresses)) + 1
        old_count = len(id_set & set(self.old_addresses)) + 1

        new_ok = (new_count >= (len(self.addresses)) / 2 + 1)
        old_ok = (len(self.old_addresses) == 0 or (old_count >= (len(self.old_addresses)) / 2 + 1))

        return new_ok and old_ok

    def append_entries(self, entries, append_from = None):
        self.log_mutex.acquire()
        if append_from != None:
            self.log = self.log[:append_from + 1]
        for en in entries:
            self.log.append(en)
            print "append: ", en.to_string()
            entry_index = len(self.log) - 1
            # after append one entry, might need to do more, especially when changing configs

            # if I am leader, prepare for append entry vote
            if self.host_id == self.leader_id:
                self.index_append_count[entry_index] = set()
            # extra things to do when config changes
            if en.command.startswith('N'):
                self.old_addresses = {}
            if en.command.startswith('ON'):
                self.decode_config_str(en.command)
        self.log_mutex.release()
        return entry_index # index of last appended entry

    def commit_entry(self):

        self.commit_mutex.acquire()
        # every time we try to commit a entry that has not been committed
        next_commit_index = len(self.committed_entry_result)

        if next_commit_index in self.index_append_count:
            del self.index_append_count[next_commit_index]

        if self.log[next_commit_index].command[0].isdigit():
            cmd_str = str(self.log[next_commit_index].command)
            num = int(cmd_str.split(',')[0])
            if num <= self.tickets_left:
                self.tickets_left -= num
                self.committed_entry_result.append(True)
                print "commit:", next_commit_index, "True, tickets:", self.tickets_left
            else:
                self.committed_entry_result.append(False)
                print "commit:", next_commit_index, "False, tickets:", self.tickets_left

        elif self.log[next_commit_index].command.startswith('ON'): # config change
            self.committed_entry_result.append('ON')
            print "commit config change:", next_commit_index, "Old + New"
            if self.host_id == self.leader_id:
                self.append_entries( [Entry(self.current_term, 'N')] )

        elif self.log[next_commit_index].command.startswith('N'): # config change
            self.committed_entry_result.append('N')
            # after config change, if leader is not in the new config, after New committer, leader step down
            if self.state == LEADER and (self.host_id not in self.addresses):
                self.change_state(FOLLOWER)
                print 'New config have been committed and I am not in the new config.'
                print 'I was LEADER!! How could this happen?'
                print 'I am so sad, Will Terminate Soon! Bye'
                """
                seems that this wont kill all the progrom...still need to kill the program manually
                otherwise, some thread will be bring alive again when add this node into the new config
                and some thread will be dead..and this will cause strange problem
                """
                global KILL
                KILL = True
                os._exit(1)
            print "commit config change:", next_commit_index, "New"
            print "New Config: ", self.addresses

        #print self.show_rpc()
        self.commit_mutex.release()

    def check_commit(self):
        while True:
            if KILL:
                return
            # keep checking whether one entry can be committed
            if self.state == LEADER:
                for index in sorted(self.index_append_count.keys()):
                    if self.check_majority(self.index_append_count[index]):
                        self.commit_entry()
                    else:
                        break

    def check_election_timeout(self):
        # True if timeout
        return (self.last_update + self.election_timeout) < time.time()

    def reset_election_timeout(self):
        self.last_update = time.time()
        self.election_timeout = random.uniform(TIMEOUT_LOW, TIMEOUT_HIGH)

    def send_an_append_entries(self, node_id, next_index = 0):
        # incase of config change
        if node_id in self.addresses:
            ip, port = self.addresses[node_id]
        else:
            ip, port = self.old_addresses[node_id]

        url = 'http://' + str(ip) + ':' + str(port)
        try:
            s = xmlrpclib.ServerProxy(url)

            prev_log_index = next_index - 1
            if len(self.log) > prev_log_index and prev_log_index >= 0:
                prev_log_term = self.log[prev_log_index].term
            else:
                prev_log_term = -1

            commited_index = len(self.committed_entry_result) - 1
            # Flag
            success = False

            # repair all entries
            while not success:
                if KILL:
                    return

                if self.state != LEADER:
                    return

                self.log_mutex.acquire()
                """
                leader maintains the next_index for each follower, which is the index leader knows about followers' log
                here we use prev_log_index + 1, which equals to next_index, but we mean need to update this by updating prev_log_index
                everytime, leader send its own log from prev_log_index + 1 -> len(log) to the follower
                when the prev_log_index + 1 == len(log), this is a heartbeat
                """
                entries_to_send = [en.to_string() for en in self.log[prev_log_index + 1:]]
                """
                # [] means a heartbeat
                # if the prev_log is not matched, the follower will send back False, and leader decrease the prev_log_index of this follower by 1, adding one more entry
                # and try again
                """
                target_term, success = s.append_entries_rpc(self.current_term, self.host_id, prev_log_index, prev_log_term, commited_index, entries_to_send)
                #print 'send', entries_to_send, 'to', node_id, ", and get respone ", success, target_term
                self.log_mutex.release()
                if success:
                    self.followers_next_index[node_id] = prev_log_index + 1 + len(entries_to_send)
                    break
                # if append returns a False, update prev_log_index and prev_log_term
                prev_log_index -= 1

                if len(self.log) > prev_log_index and prev_log_index >= 0:
                    prev_log_term = self.log[prev_log_index].term
                else:
                    prev_log_term = -1

            # update index_append_count
            for i in range(commited_index + 1, len(self.log)):
                self.commit_mutex.acquire()
                if i > len(self.committed_entry_result) - 1:
                    self.index_append_count[i].add(node_id)
                self.commit_mutex.release()

        except Exception as e:
            self.log_mutex.release()
            #print "could not send heartbeat to %s.. Exception: %s" % (str(port), str(e))

    def send_a_request_vote_rpc(self, node_id):
        if node_id in self.addresses:
            ip, port = self.addresses[node_id]
        else:
            ip, port = self.old_addresses[node_id]
        url = 'http://' + str(ip) + ':' + str(port)
        success = False
        if len(self.log) > 0:
            term = self.log[-1].term
        else:
            term = -1

        while (not self.check_election_timeout()) and (not success) and self.state == CANDIDATE:
            if KILL:
                return

            try:
                s = xmlrpclib.ServerProxy(url)
                term_return, vote_granted = s.request_vote_rpc(self.host_id, self.current_term, len(self.log) - 1, term)
                success = True
                if vote_granted:
                    self.granted_votes_mutex.acquire()
                    self.granted_votes.add(node_id)
                    self.granted_votes_mutex.release()
            except Exception as e:
                #print "could not connect %s.. Exception: %s" % (str(port), str(e))
                time.sleep(0.5)

    def change_state(self, state):
        self.state = state
        #self.election_timeout = random.uniform(TIMEOUT_LOW, TIMEOUT_HIGH)
        print "[" + str(time.time()) + "] " "Changed state to " + str(state) + " in term: " + str(self.current_term)

    def get_a_rpc_connection_to_leader(self):
        ip = self.addresses[self.leader_id][0]
        port = self.addresses[self.leader_id][1]
        url = 'http://' + str(ip) + ':' + str(port)
        try:
            s = xmlrpclib.ServerProxy(url)
            return s
        except Exception as e:
            #print "could not reach leader at %s.. Exception: %s" % (str(port), str(e))
            pass

    # Client rpc

    def buy_ticket_rpc(self, num, address_client_connected_to):
        # if I am the leader
        if self.host_id == self.leader_id:
            # a new entry
            cmd = str(num) + ', bought by ' + str(address_client_connected_to)
            entry = Entry(self.current_term, cmd)
            entry_index = self.append_entries([entry])

            while entry_index > len(self.committed_entry_result) - 1:
                if KILL:
                    return
            while self.committed_entry_result[entry_index] == None:
                continue

            if self.committed_entry_result[entry_index]:
                return 'Successfully buy '+ num + ' tickets'
            else:
                return 'Sorry, bro, failed this time, try again'

        else:
            # forward it to the leader
            rpc_connection = self.get_a_rpc_connection_to_leader()
            try:
                return rpc_connection.buy_ticket_rpc(num, address_client_connected_to)
            except Exception as e:
                return 'Could not reach the leader, wait a sec'
                #print "could not reach leader .. Exception: %s" % str(e)
                pass

    def show_rpc(self):
        return self.tickets_left, [en.to_string() for en in self.log], self.committed_entry_result

    # config -> str
    def encode_config_str(self, servers = []):
        new_str = ""
        # used for convert self.addresses to string
        if len(servers) == 0:
            for node_id in self.addresses:
                new_str += "%d,%s,%d " % (node_id, self.addresses[node_id][0], self.addresses[node_id][1])
        else:
            # used for convert new configs to string
            for triple in servers:
                new_str += "%s,%s,%s " % tuple(triple)
        return new_str

    # str -> config
    def decode_config_str(self, command):
        addresses = {}
        old_addresses = {}
        split_cmd = command.split("|")
        for single_str in split_cmd[1].strip().split():
            node_id, ip, port = single_str.split(",")
            old_addresses[ int(node_id) ] = (ip, int(port))

        for single_str in split_cmd[2].strip().split():
            node_id, ip, port = single_str.split(",")
            addresses[int(node_id)] = (ip, int(port))

        for node_id in set(old_addresses) | set(addresses):
            if node_id != self.host_id and (node_id not in self.followers_next_index):
                self.followers_next_index[node_id] = 0

        self.old_addresses = old_addresses
        self.addresses = addresses

    # [(id, ip, port)]
    def add_server_rpc(self, servers_tuple_list):
        # if I am the leader
        if self.host_id == self.leader_id:
            # a new entry
            str_old = self.encode_config_str([])
            str_new = self.encode_config_str(servers_tuple_list)
            command = "ON|%s|%s" % (str_old, str_new)

            entry = Entry(self.current_term, command)
            self.append_entries( [entry] )

        else:
            # forward it to the leader
            rpc_connection = self.get_a_rpc_connection_to_leader()
            try:
                rpc_connection.add_server_rpc(servers_tuple_list)
            except Exception as e:
                #print "could not reach leader .. Exception: %s" % str(e)
                pass

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

    host_ip = None
    host_port = None

    # get nodes config
    for aline in fin:
        split_line = aline.strip().split()

        node_id = int(split_line[0])
        ip = split_line[1]
        port = int(split_line[2])

        if node_id == host_id:
            host_ip = ip
            host_port = port

        addresses[node_id] = (str(ip), port)

    # addresses will include host itself
    datacenter_obj = Datacenter(host_id, host_ip, host_port, addresses, tickets_left)

    return datacenter_obj


if __name__ == '__main__':
    host_id = int(sys.argv[1])
    if len(sys.argv) == 3:
        config_filename = sys.argv[2]
        datacenter_obj = cluster_init(config_filename=config_filename, host_id=host_id)
    else:
        print "wrong params"
        sys.exit()
    datacenter_obj.start()

    # start rpc server
    try:
        server = SimpleThreadedXMLRPCServer(("localhost", datacenter_obj.port), allow_none=True, logRequests=False)
        print "Listening on port %d..." % datacenter_obj.port
        server.register_multicall_functions()
        server.register_instance(datacenter_obj)
        server.serve_forever()
    except KeyboardInterrupt:
        print "#########################################"
        KILL = True
        sys.exit()
