import os
import json
import random
import time

import yaml
import socket
import threading

class Service:
    def __init__(self, id: int, config_path: str, sleep=0):
        self.node_id = id
        self.udp_thread = None
        self.udp_sock = None
        self.sleep = sleep

        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.dict = {}
        self.commit_idx = 0
        self.last_applied_idx = 0
        self.routes = {}
        self.my_addr = None
        self.state = 'follower'
        self.votes = {}
        self.leader_id = None
        self.next_idx = {}
        self.match_idx = {}

        self.config_internet(config_path)

        self.init_udp_recv_settings()
        self.stop_udp_thread = False
        self.start_listening()

        # time clock
        self.T = (5,10)
        self.election_timeout = time.time() + random.randint(*self.T)
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

    def generate_packet_to_send(self, msg_item: str, msg_type: str) -> dict:
        """
        Packet definition:
        packet = {
            'type': [],
            'item': str (can be a dumped json string)
        }
        :param msg_item:
        :param msg_type:
        :return:
        """
        packet = {
            'type': msg_type,
            'item': msg_item,
            'from': self.node_id
        }
        return packet

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
        try:
            # Send the packet
            self.message_delay()
            sock.sendto(data.encode(), addr)
        finally:
            # Close the socket
            sock.close()

    def message_delay(self):
        if self.sleep == -1:
            sleep_time = random.uniform(0, 3)
            time.sleep(sleep_time)
        elif self.sleep:
            time.sleep(self.sleep)

    def send_append_entries(self, peer):
        # TODO: Implement send_append_entries RPC to peer

    def request_vote(self):
        for dst in self.routes:
            if not self.votes[dst]:
                request = {
                    'type': 'request_vote',
                    'src': self.node_id,
                    'dst': dst,
                    'term': self.current_term,
                    'candidate_id': self.node_id,
                    'last_log_index': self.log.last_log_index,
                    'last_log_term': self.log.last_log_term
                }
                request = json.dumps(request)

                if self.routes['dst']['enable']:
                    self.send_udp_packet(request,self.routes['dst']['addr'])

    def respond_vote(self,data):
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

            return

        candidate_id = data['candidate_id']
        last_log_index = data['last_log_index']
        last_log_term = data['last_log_term']

        if not self.voted_for or self.voted_for == candidate_id:
            if last_log_index >= self.log.last_log_index and last_log_term >= self.log.last_log_term:
                self.voted_for = data['src']
                response['grant'] = True
                response = json.dumps(response)

                if self.routes[data['src']]['enable']:
                    self.send_udp_packet(response, self.routes[data['src']]['addr'])
            else:
                self.voted_for = None
                response['grant'] = False
                response = json.dumps(response)

                if self.routes[data['src']]['enable']:
                    self.send_udp_packet(response, self.routes[data['src']]['addr'])
        else:
            response['grant'] = False
            response = json.dumps(response)

            if self.routes[data['src']]['enable']:
                self.send_udp_packet(response, self.routes[data['src']]['addr'])
        return




    def init_udp_recv_settings(self):
        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_sock.bind(self.my_addr)

    def start_listening(self):
        # Start the thread to listen for UDP packets
        self.udp_thread = threading.Thread(target=self.listen_for_udp)
        self.udp_thread.start()

    def redirect(self,data,addr):
        pass

    def all_server_func(self,data):
        if self.commit_idx > self.last_applied_idx:
            self.last_applied_idx = self.commit_idx
            ######## applied to local machine ############
            pass

        if data['term'] > self.current_term:
            self.current_term = data['term']
            self.state = 'follower'
            self.voted_for = None

    def follower_func(self,data):
        t = time.time()
        # func 1: respond to RPC
        if data['type'] == 'AppendEntries':
            ############################## need ###################
            pass

        elif data['type'] == 'RequestVote':
            self.respond_vote(data)

        # func 2: start election if timeout
        if t > self.election_timeout:
            self.election_timeout = t + random.randint(*self.T)
            self.state = 'candidate'
            self.current_term += 1
            self.voted_for = self.node_id
            self.votes = { voter: False for voter in self.routes }
            self.votes[self.node_id] = True

    def candidate_func(self,data):
        t = time.time()

        # start election
        self.request_vote()

        if data['term'] == self.current_term:
            if data['type'] == 'ResponseVote':
                self.votes[data['src']] = data['grant']
                vote_count = sum(list(self.votes.values()))

                if vote_count >= len(self.routes)//2:
                    self.state = 'leader'
                    self.voted_for = None
                    self.heartbeat = 0
                    self.next_idx = {_id: self.log.last_log_index + 1 for _id in self.routes}
                    self.match_idx = {_id: 0 for _id in self.routes}
                    return
            elif data['type'] == 'AppendEntries':
                self.election_timeout = t + random.randint(*self.T)
                self.state = 'follower'
                self.voted_for = None
                return

        if t > self.election_timeout:
            self.election_timeout = t + random.randint(*self.T)
            self.current_term += 1
            self.voted_for = self.node_id
            self.votes = {voter: False for voter in self.routes}
            self.votes[self.node_id] = True


    def leader_func(self,data):
        pass


    def listen_for_udp(self):
        while not self.stop_udp_thread:
            data, addr = self.udp_sock.recvfrom(65535)

            # data = self.redirect(data, addr)

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

