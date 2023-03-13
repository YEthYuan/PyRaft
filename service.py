import sys
import json
import random
import time

import yaml
import socket
import threading

from log import Log

class Service:
    def __init__(self, id: int, config_path: str, sleep=1):
        self.node_id = id
        self.udp_thread = None
        self.udp_sock = None
        self.sleep = sleep

        self.current_term = 0
        self.voted_for = None
        self.filename = str(self.node_id)+"_log.json"
        self.log = Log(self.filename)
        self.dict_id = (self.node_id,0)
        self.dict = {}
        self.commit_idx = 0
        self.last_applied_idx = 0
        self.routes = {}
        self.my_addr = None
        self.state = 'follower'
        self.votes = {}
        self.leader_id = None

        # volatile state on leaders
        self.next_idx = {_id: self.log.last_log_index+1 for _id in self.routes}
        self.match_idx = {_id: -1 for _id in self.routes}

        self.config_internet(config_path)

        # udp socket setting
        self.init_udp_recv_settings()
        self.stop_udp_thread = False
        self.start_listening()

        # time clock
        # self.election_timeout = time.time() + random.randint(*self.T)
        self.election_timeout = time.time() + (7 + self.node_id * 3)
        self.heartbeat = 0

    def config_internet(self, path: str):
        with open(path, 'r') as file:
            config = yaml.load(file, Loader=yaml.FullLoader)
        print(f"==>Config file loaded from {path}")

        for client in config['clients']:
            if self.node_id == client['nodeId']:
                self.my_addr = (client['ip'], client['port'])

            self.routes[client['nodeId']] = {
                'addr': (client['ip'], client['port']),
                'enable': True
            }

    # def generate_packet_to_send(self, msg_item: str, msg_type: str) -> dict:
    #     """
    #     Packet definition:
    #     packet = {
    #         'type': [],
    #         'item': str (can be a dumped json string)
    #     }
    #     :param msg_item:
    #     :param msg_type:
    #     :return:
    #     """
    #     packet = {
    #         'type': msg_type,
    #         'item': msg_item,
    #         'from': self.node_id
    #     }
    #     return packet
    def send_udp_packet(self, data: str, addr: tuple):
        """
        Sends a UDP packet to a specified host and port

        Args:
        data: str: the message you want to send
        host: str: the destination IP address
        port: int: the destination port

        Returns:
        None

        """
        # Create a socket object
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(data.encode(), addr)

    def message_delay(self):
        if self.sleep == -1:
            sleep_time = random.uniform(0, 3)
            time.sleep(sleep_time)
        elif self.sleep:
            time.sleep(self.sleep)

    def append_entries(self):
        self.message_delay()

        print(self.next_idx)
        print(self.match_idx)

        for dst in self.routes:
            if dst != self.node_id:
                append_request = {
                    'type': 'AppendEntries',
                    'src': self.node_id,
                    'dst': dst,
                    'term' : self.current_term,
                    'leader_id':self.node_id,
                    'prev_log_index': self.next_idx[dst] - 1,
                    'prev_log_term': self.log.get_entry_term(self.next_idx[dst] - 1),
                    'entries': self.log.get_entries(self.next_idx[dst]),
                    'leader_commit': self.commit_idx
                }
                append_request = json.dumps(append_request)

                if self.routes[dst]['enable']:
                    print(f"[Log]: leader {self.node_id} send append_entry to follower {dst}")
                    self.send_udp_packet(append_request, self.routes[dst]['addr'])

    def respond_append(self, data):
        self.message_delay()
        response = {
            'type': 'ResponseAppend',
            'term': self.current_term,
            'src': self.node_id,
            'dst': data['src'],
            'success': False
        }

        if data['term'] < self.current_term:
            response['success'] = False

            if self.routes[data['src']]['enable']:
                response = json.dumps(response)
                print(f"[Log]: follower {self.node_id} response to leader {data['src']}")
                self.send_udp_packet(response, self.routes[data['src']]['addr'])

            return False

        if data['entries']:
            print(f"prev_log_index:{data['prev_log_index']}")
            print(f"prev_log_term:{data['prev_log_term']}")
            prev_log_index = data['prev_log_index']
            prev_log_term = data['prev_log_term']

            if prev_log_term != self.log.get_entry_term(prev_log_index):
                response['success'] = False
                self.log.log_delete(prev_log_index)
            else:
                response['success'] = True
                self.log.log_append(data['entries'])

            print(f"response: {response['success']}")
            if self.routes[data['src']]['enable']:
                response = json.dumps(response)
                print(f"[Log]: follower {self.node_id} response to leader {data['src']}")
                self.send_udp_packet(response, self.routes[data['src']]['addr'])

        leader_commit = data['leader_commit']
        if leader_commit > self.commit_idx:
            self.commit_idx = min(self.log.last_log_index, leader_commit)

        self.leader_id = data['leader_id']

        return True

    def start_election(self):
        # self.election_timeout = time.time() + random.randint(*self.T)
        self.election_timeout = time.time() + (7 + self.node_id * 3)
        self.state = 'candidate'
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes = {voter: False for voter in self.routes}
        self.votes[self.node_id] = True

    def request_vote(self):
        self.message_delay()

        for dst in self.routes:
            if not self.votes[dst]:
                request = {
                    'type': 'RequestVote',
                    'src': self.node_id,
                    'dst': dst,
                    'term': self.current_term,
                    'candidate_id': self.node_id,
                    'last_log_index': self.log.last_log_index,
                    'last_log_term': self.log.last_log_term
                }
                request = json.dumps(request)

                if self.routes[dst]['enable']:
                    print(f"[Log]: candidate {self.node_id} request vote from {dst}")
                    self.send_udp_packet(request, self.routes[dst]['addr'])

    def respond_vote(self, data):
        self.message_delay()
        response = {
            'type': 'ResponseVote',
            'src': self.node_id,
            'dst': data['src'],
            'term': self.current_term,
            'grant': False
        }
        if data['term'] < self.current_term:
            response['grant'] = False
            response = json.dumps(response)

            if self.routes[data['src']]['enable']:
                self.send_udp_packet(response,self.routes[data['src']]['addr'])

            return False

        candidate_id = data['candidate_id']
        last_log_index = data['last_log_index']
        last_log_term = data['last_log_term']

        # print(f"current term :{self.current_term}")
        # print(f"data term: {data['term']}")

        if not self.voted_for or self.voted_for == candidate_id:
            if last_log_index >= self.log.last_log_index and last_log_term >= self.log.last_log_term:
                self.voted_for = data['src']
                response['grant'] = True
                response = json.dumps(response)

                if self.routes[data['src']]['enable']:
                    print(f"[Log]: follower {self.node_id} vote for candidate {data['src']} ")
                    self.send_udp_packet(response, self.routes[data['src']]['addr'])

                return True
            else:
                self.voted_for = None
                response['grant'] = False
                response = json.dumps(response)

                if self.routes[data['src']]['enable']:
                    self.send_udp_packet(response, self.routes[data['src']]['addr'])

                return False
        else:
            response['grant'] = False
            response = json.dumps(response)

            if self.routes[data['src']]['enable']:
                self.send_udp_packet(response, self.routes[data['src']]['addr'])

            return True

    def init_udp_recv_settings(self):
        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_sock.bind(self.my_addr)
        self.udp_sock.settimeout(2)

    def start_listening(self):
        # Start the thread to listen for UDP packets
        self.udp_thread = threading.Thread(target=self.listen_for_udp)
        self.udp_thread.start()

    def all_server_func(self,data):
        if self.commit_idx > self.last_applied_idx:
            print(f"[Log]:Apply Entry {self.commit_idx} on Server{self.node_id}")
            self.last_applied_idx = self.commit_idx
            ######## applied to local machine ############
            pass

        if not data:
            return

        if data['type'] != 'ClientRequest':
            print(f"[Log]: datatype {data['type']} from {data['src']} to {data['dst']}")

        print(f"current term :{self.current_term}")
        print(f"data term: {data['term']}")

        if data['term'] > self.current_term:
            self.current_term = data['term']
            self.state = 'follower'
            self.voted_for = None

    def follower_func(self, data):
        timeout = False
        # func 1: respond to RPC
        if data:
            if data['type'] == 'AppendEntries':
                print(f"[Log]: recv append_entry rpc from leader {data['src']}")
                timeout = self.respond_append(data)

            elif data['type'] == 'RequestVote':
                print(f"[Log]: recv request_vote rpc from candidate {data['src']}")
                timeout = self.respond_vote(data)

        # print(timeout)
        if timeout:
            # self.election_timeout = time.time() + random.randint(*self.T)
            self.election_timeout = time.time() + (7 + self.node_id * 3)

        # func 2: start election if timeout
        if time.time() > self.election_timeout:
            print(f"[Log]: follower {self.node_id} start an election")
            self.start_election()

    def candidate_func(self, data):

        # start election
        self.request_vote()
        if data and data['term'] == self.current_term:
            if data['type'] == 'ResponseVote':
                self.votes[data['src']] = data['grant']
                if data['grant']:
                    print(f"[Log]: candidate {self.node_id} receive vote from {data['src']}")
                vote_count = sum(list(self.votes.values()))

                # print(f"vote_count:{vote_count}")
                if vote_count > len(self.routes)//2:
                    print(f"[Log]: candidate {self.node_id} become a leader")
                    self.state = 'leader'
                    self.voted_for = None
                    self.heartbeat = 0
                    self.next_idx = {_id: self.log.last_log_index + 1 for _id in self.routes}
                    self.match_idx = {_id: 0 for _id in self.routes}
                    return
            elif data['type'] == 'AppendEntries':
                print(f"[Log]: candidate {self.node_id} receive append entry rpc from leader {data['src']}")
                print(f"[Log]: candidate {self.node_id} become a follower")
                # self.election_timeout = time.time() + random.randint(*self.T)
                self.election_timeout = time.time() + (7 + self.node_id * 3)
                self.state = 'follower'
                self.voted_for = None
                return

        if time.time() > self.election_timeout:
            print(f"[Log]: candidate {self.node_id} start an election")
            self.start_election()

    def leader_func(self, data):

        if time.time() > self.heartbeat:
            # self.heartbeat = time.time()+random.randint(*(0, 5))
            self.heartbeat = time.time() + 3
            self.append_entries()

        if data != None and data['type'] == 'ClientRequest':
            print(f"[Log]: leader {self.node_id} receive client request")
            if data['command'] == 'create':
                act = 'create'
                info = {
                    'dict_id': self.dict_id,
                    'clients_id': data['clients_id']
                    # need to add public key and private key here
                }
            elif data['command'] == 'put':
                act = 'put'
                info = {
                    'dict_id': data['dict_id'],
                    'client_id': data['client_id'],
                    'key': data['key'],
                    'value': data['value']
                    # need to replace by encrypted version
                }
            elif data['command'] == 'get':
                act = 'get'
                info = {
                    'dict_id': data['dict_id'],
                    'client_id': data['client_id'],
                    'key': data['key']
                    # need to replace by encrypted version
                }
            new_entry = {
                'term': self.current_term,
                'act': act,
                'info': info
            }
            self.log.log_append([new_entry])

            return

        if data != None and data['type'] == 'ResponseAppend':
            if not data['success']:
                print(f"[Log]: leader {self.node_id} receive failure response from follower {data['src']}")
                self.next_idx[data['src']] -= 1
            else:
                print(f"[Log]: leader {self.node_id} receive success response from follower {data['src']}")
                self.match_idx[data['src']] = self.next_idx[data['src']]
                print(self.log.last_log_index)
                print(self.log.entries)
                self.next_idx[data['src']] = self.log.last_log_index + 1

        while True:
            N = self.commit_idx + 1
            count = 0
            for peer in self.match_idx:
                if self.match_idx[peer] >= N:
                    count += 1
                if count > len(self.routes)//2:
                    self.commit_idx = N
                    print(f"[Log]: leader {self.node_id} commit entry {self.commit_idx}")
                    # run on local machine

                    break
            else:
                break

    def deal_clients(self, data):
        if data['command'] == 'create' or data['command'] == 'put' or data['command'] == 'get':
            data['term'] = self.current_term
            if self.state != 'leader':
                if self.leader_id:
                    data = json.dumps(data)
                    if self.routes[self.leader_id]['enable']:
                        print(f"follower {self.node_id} redirect data to leader {self.leader_id}")
                        self.send_udp_packet(data, self.routes[self.leader_id]['addr'])
                        return None
        elif data['command'] == 'printDict':
            print("DICT")
            # need to response user using user_addr
            return None
        elif data['command'] == 'printAll':
            print("Alldict")
            # need to response user using user_addr
            return None
        elif data['command'] == 'failLink':
            self.routes[data['dest']]['enable'] = False
            # need to response user using user_addr
            return None
        elif data['command'] == 'fixLink':
            self.routes[data['dest']]['enable'] = True
            # need to response user using user_addr
            return None
        else:
            print('kill precess')
            # need to response user using user_addr
            return None

        return data

    def listen_for_udp(self):
        while not self.stop_udp_thread:
            try:
                data, addr = self.udp_sock.recvfrom(2048)
            except Exception as e:
                data, addr = None, None

            if data:
                data = json.loads(data)
                if data['type'] == 'ClientRequest':
                    print(f"[Log]: receive user command {data['command']}")
                    data = self.deal_clients(data)

            self.all_server_func(data)

            if self.state == 'follower':
                self.follower_func(data)

            if self.state == 'candidate':
                self.candidate_func(data)

            if self.state == 'leader':
                self.leader_func(data)

    def stop_udp(self):
        self.stop_udp_thread = True
        self.udp_thread.join()



def run():
    user_id = int(sys.argv[1])
    config_path = "config.yaml"

    service = Service(user_id, config_path)


if __name__ == '__main__':
    run()

