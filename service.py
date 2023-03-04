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

        self.term = 0
        self.voted_for = None
        self.log = []
        self.dict = {}
        self.commit_idx = 0
        self.last_applied_idx = 0
        self.routes = {}
        self.my_addr = None
        self.state = 'follower'
        self.election_timeout = None
        self.vote_count = 0
        self.leader_id = None
        self.next_idx = {}
        self.match_idx = {}

        self.config_internet(config_path)

        self.init_udp_recv_settings()
        self.start_listening()


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

    def message_delay(self):
        if self.sleep == -1:
            sleep_time = random.uniform(0, 3)
            time.sleep(sleep_time)
        elif self.sleep:
            time.sleep(self.sleep)

    def start(self):
        self.election_timeout = threading.Timer(self.random_timeout(), self.start_election)
        self.election_timeout.start()

    def stop(self):
        self.election_timeout.cancel()

    def random_timeout(self):
        # TODO: Implement randomized election timeout
        pass

    def start_election(self):
        self.state = 'candidate'
        self.term += 1
        self.voted_for = self.node_id
        self.vote_count = 1
        self.election_timeout.cancel()

        for peer in self.peers:
            # TODO: Implement send_request_vote RPC to peers

        self.election_timeout = threading.Timer(self.random_timeout(), self.start_election)
        self.election_timeout.start()

    def on_request_vote(self, candidate_id, term, last_log_index, last_log_term):
        if term < self.term:
            return False

        if self.voted_for is None or self.voted_for == candidate_id:
            if self.log[-1].term <= last_log_term and len(self.log) <= last_log_index:
                self.voted_for = candidate_id
                return True

        return False

    def on_append_entries(self, leader_id, term, prev_log_index, prev_log_term, entries, leader_commit):
        if term < self.term:
            return False

        self.election_timeout.cancel()
        self.state = 'follower'
        self.term = term
        self.leader_id = leader_id

        if prev_log_index >= len(self.log) or self.log[prev_log_index].term != prev_log_term:
            return False

        for i in range(len(entries)):
            if prev_log_index + 1 + i >= len(self.log) or self.log[prev_log_index + 1 + i].term != entries[i].term:
                self.log = self.log[:prev_log_index + 1 + i]
                self.log.extend(entries[i:])
                break

        if leader_commit > self.commit_idx:
            self.commit_idx = min(leader_commit, len(self.log) - 1)

        return True

    def send_append_entries(self, peer):
        # TODO: Implement send_append_entries RPC to peer

    def send_request_vote(self, peer):
        # TODO: Implement send_request_vote RPC to peer


    ##############  Network Services  ################################
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
            sock.sendto(data.encode(), addr)
        finally:
            # Close the socket
            sock.close()

    def init_udp_recv_settings(self):
        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_sock.bind(self.my_addr)

    def start_listening(self):
        # Start the thread to listen for UDP packets
        self.udp_thread = threading.Thread(target=self.listen_for_udp)
        self.udp_thread.start()

    def listen_for_udp(self):
        while not self.stop_udp_thread:
            data, addr = self.udp_sock.recvfrom(1024)
            # print("Received data:", data)
            # print("From address:", addr)
            self.process_recv_data(data)

    def stop_udp(self):
        self.stop_udp_thread = True
        self.udp_thread.join()

    def process_recv_data(self, data):
        data = json.loads(data)
        if data['type'] == 'XXX':
            if self.state != "":
                return
            print(f"==>Received XXX from another client {data['from']}.")
            payload = data['item']
            payload = json.loads(payload)
            self.process_XXX(payload, sender=data['from'])
        elif data['type'] == 'YYY':
            if self.state != "":
                return
            print(f"")
            payload = data['item']
            payload = json.loads(payload)
            self.processYYY(payload)
        elif data['type'] == 'client-release':
            if self.state != "":
                return
            print(f"==>Client {data['from']} sent you a release.")
            payload = data['item']
            payload = json.loads(payload)
            self.process_ZZZ(payload)
