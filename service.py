import base64
import os
import pickle
import sys
import json
import random
import time
import rsa

import yaml
import socket
import threading

from log import Log

from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad

class Service:
    def __init__(self, id, config_path: str, recovery=0, sleep=1):
        self.node_id = id
        self.pid = 0
        self.udp_thread = None
        self.udp_sock = None
        self.sleep = sleep
        self.routes = {}
        self.my_addr = None
        # generate key pair
        self.public_key = None
        self.private_key = None
        self.pubkey_list = None
        self.config_internet(config_path)

        self.current_term = 0
        self.voted_for = None
        # self.filename = "logs/" + self.node_id + "_log.json"
        self.log = Log()
        self.dict_id = (self.pid, 0)
        self.dict = {}
        self.commit_idx = -1
        self.last_applied_idx = -1

        self.state = 'follower'
        self.votes = {}
        self.leader_id = None

        # volatile state on leaders
        self.next_idx = {_id: self.log.last_log_index+1 for _id in self.routes}
        self.match_idx = {_id: -1 for _id in self.routes}

        # udp socket setting
        self.init_udp_recv_settings()
        self.stop_udp_thread = False
        self.start_listening()

        # time clock
        # self.election_timeout = time.time() + random.randint(*self.T)
        self.election_timeout = time.time() + (7 + self.pid * 3)
        self.heartbeat = 0

        if recovery:
            self.load_ckpt()

    def config_internet(self, path: str):
        self.pubkey_list = {}
        with open(path, 'r') as file:
            config = yaml.load(file, Loader=yaml.FullLoader)
        print(f"==>Config file loaded from {path}")

        for client in config['clients']:
            if self.node_id == client['nodeId']:
                self.my_addr = (client['ip'], client['port'])
                self.pid = client['pid']
                public_path = "secrets/node_" + self.node_id + "_public_key.pem"
                private_path = "secrets/node_" + self.node_id + "_private_key.pem"
                with open(public_path, mode='rb') as file:
                    public_key_data = file.read()
                self.public_key = rsa.PublicKey.load_pkcs1(public_key_data)

                with open(private_path, mode='rb') as file:
                    private_key_data = file.read()
                self.private_key = rsa.PrivateKey.load_pkcs1(private_key_data)

            self.routes[client['nodeId']] = {
                'addr': (client['ip'], client['port']),
                'enable': True
            }
            public_path = "secrets/node_" + str(client['nodeId']) + "_public_key.pem"
            with open(public_path, mode='rb') as file:
                public_key_data = file.read()
            self.pubkey_list[client['nodeId']] = rsa.PublicKey.load_pkcs1(public_key_data)

    def gen_rsa_key_pair(self, length=2048, path="secrets/"):
        public_path = path + "node_" + str(self.node_id) + "_public_key.pem"
        private_path = path + "node_" + str(self.node_id) + "_private_key.pem"
        if os.path.exists(public_path) and os.path.exists(private_path):
            with open(public_path, mode='rb') as file:
                public_key_data = file.read()
            self.public_key = rsa.PublicKey.load_pkcs1(public_key_data)

            with open(private_path, mode='rb') as file:
                private_key_data = file.read()
            self.private_key = rsa.PrivateKey.load_pkcs1(private_key_data)

        else:
            (self.public_key, self.private_key) = rsa.newkeys(length)
            with open(public_path, "wb") as public_file:
                public_file.write(self.public_key.save_pkcs1())

            with open(private_path, "wb") as private_file:
                private_file.write(self.private_key.save_pkcs1())

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
    def send_udp_packet(self, data: str, addr, dst):
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
        data = data.encode("utf-8")
        # data_enc = rsa.encrypt(data, self.pubkey_list[dst])

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(data, tuple(addr))

    def save_ckpt(self):
        del self.__dict__['udp_thread']
        del self.__dict__['udp_sock']
        filename = "ckpts/node" + str(self.node_id) + "_ckpt.pkl"
        with open(filename, 'wb') as file:
            pickle.dump(self.__dict__, file)

        print(f"==> Checkpoint save to {filename}")

    def load_ckpt(self, filename=None):
        if filename == None:
            filename = "ckpts/node" + str(self.node_id) + "_ckpt.pkl"

        with open(filename, 'rb') as file:
            node_dict = pickle.load(file)

        self.__dict__.update(node_dict)
        print(f"==> Recovery from {filename}")

    def message_delay(self):
        if self.sleep == -1:
            sleep_time = random.uniform(0, 3)
            time.sleep(sleep_time)
        elif self.sleep:
            time.sleep(self.sleep)

    def append_entries(self):
        self.message_delay()

        print(f"next_idx:{self.next_idx}")

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
                    self.send_udp_packet(append_request, self.routes[dst]['addr'], dst)

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
                self.send_udp_packet(response, self.routes[data['src']]['addr'], data['src'])

            return False

        if data['entries']:
            print(f"prev_log_index:{data['prev_log_index']}")
            print(f"prev_log_term:{data['prev_log_term']}")
            prev_log_index = data['prev_log_index']  # value from server
            prev_log_term = data['prev_log_term']  # value from server

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
                self.send_udp_packet(response, self.routes[data['src']]['addr'], data['src'])

        leader_commit = data['leader_commit']
        if leader_commit > self.commit_idx:
            self.commit_idx = min(self.log.last_log_index, leader_commit)

        self.leader_id = data['leader_id']

        return True

    def start_election(self):
        # self.election_timeout = time.time() + random.randint(*self.T)
        self.election_timeout = time.time() + (7 + self.pid * 3)
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
                    self.send_udp_packet(request, self.routes[dst]['addr'], dst)

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
                self.send_udp_packet(response,self.routes[data['src']]['addr'], data['src'])

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
                    self.send_udp_packet(response, self.routes[data['src']]['addr'], data['src'])

                return True
            else:
                self.voted_for = None
                response['grant'] = False
                response = json.dumps(response)

                if self.routes[data['src']]['enable']:
                    self.send_udp_packet(response, self.routes[data['src']]['addr'], data['src'])

                return False
        else:
            response['grant'] = False
            response = json.dumps(response)

            if self.routes[data['src']]['enable']:
                self.send_udp_packet(response, self.routes[data['src']]['addr'], data['src'])

            return True

    def init_udp_recv_settings(self):
        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_sock.bind(self.my_addr)
        self.udp_sock.settimeout(2)

    def start_listening(self):
        # Start the thread to listen for UDP packets
        self.udp_thread = threading.Thread(target=self.listen_for_udp)
        self.udp_thread.start()

    def update_dict_perm(self):
        path = "dicts/node_" + str(self.node_id) + "_dict.json"
        json_str = json.dumps(self.dict)
        with open(path, 'w') as f:
            f.write(json_str)

    def reply_to_user(self, addr: tuple, payload: dict):
        json_str = json.dumps(payload)
        self.send_udp_packet(json_str, addr, None)

    def all_server_func(self,data):
        print(f"commit_idx:{self.commit_idx}")
        print(f"last_applied_idx:{self.last_applied_idx}")
        print(f"dict:{self.dict}")
        print(self.log.entries)
        if self.commit_idx > self.last_applied_idx:
            # print(f"[Log]:Apply Entry {self.commit_idx} on Server{self.node_id}")
            ######## applied to local machine ############
            for i in range(self.last_applied_idx+1, self.commit_idx+1):
                current_log = self.log.get_entry(i)
                if current_log['act'] == 'create':
                    if self.node_id in current_log['info']['clients_id']:  # the dictionary is shared on this node
                        self.dict[str(current_log['info']['dict_id'])] = current_log['info']
                        self.update_dict_perm()

                        payload = {
                            "act": "create",
                            "msg": f"[Node {self.node_id}] Successfully created dict {current_log['info']['dict_id']} on node {self.node_id}"
                        }
                        self.reply_to_user(current_log['user_addr'], payload)

                elif current_log['act'] == 'put':
                    if str(current_log['info']['dict_id']) in self.dict:  # if the current node contains the certain dict
                        op_dict = self.dict[str(current_log['info']['dict_id'])]
                        if self.node_id in op_dict['clients_id']:
                            enc_aes_key = op_dict['secret_keys'][self.node_id]
                            enc_aes_key = base64.b64decode(enc_aes_key)
                            aes_key = rsa.decrypt(enc_aes_key, self.private_key)

                            add_key = current_log['info']['key']
                            plain_val = current_log['info']['value']

                            enc_val = self.encrypt_message(plain_val, aes_key)

                            op_dict['payload'][add_key] = base64.b64encode(enc_val).decode()
                            self.dict[str(current_log['info']['dict_id'])] = op_dict
                            self.update_dict_perm()

                            res = {
                                "act": "put",
                                "msg": f"[Node {self.node_id}] Successfully modified dict {current_log['info']['dict_id']} on node {self.node_id}, set <{add_key}> to <{plain_val}>, encrypted as '{base64.b64encode(enc_val).decode()}'"
                            }
                            self.reply_to_user(current_log['user_addr'], res)

                elif current_log['act'] == 'get':
                    if str(current_log['info']['dict_id']) in self.dict:  # if the current node contains the certain dict
                        op_dict = self.dict[str(current_log['info']['dict_id'])]
                        if self.node_id in op_dict['clients_id']:  # if the current node has the permission to read the dict
                            enc_aes_key = op_dict['secret_keys'][self.node_id]
                            enc_aes_key = base64.b64decode(enc_aes_key)
                            aes_key = rsa.decrypt(enc_aes_key, self.private_key)

                            read_key = current_log['info']['key']
                            enc_val = base64.b64decode(op_dict['payload'][read_key])

                            plain_data = self.decrypt_message(enc_val, aes_key)

                            res = {
                                "act": "get",
                                "msg": f"[Node {self.node_id}] Successfully read dict {current_log['info']['dict_id']} on node {self.node_id}, the <{read_key}> === <{plain_data}>"
                            }
                            self.reply_to_user(current_log['user_addr'], res)
            self.last_applied_idx = self.commit_idx


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
            self.election_timeout = time.time() + (7 + self.pid * 3)

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
                    self.match_idx = {_id: -1 for _id in self.routes}
                    return
            elif data['type'] == 'AppendEntries':
                print(f"[Log]: candidate {self.node_id} receive append entry rpc from leader {data['src']}")
                print(f"[Log]: candidate {self.node_id} become a follower")
                # self.election_timeout = time.time() + random.randint(*self.T)
                self.election_timeout = time.time() + (7 + self.pid * 3)
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
                aes_key = self.generate_key()
                secret_keys = {}
                for c_id in data['clients_id']:
                    c_pub_key = self.pubkey_list[c_id]
                    enc_aes_key = rsa.encrypt(aes_key, c_pub_key)
                    enc_aes_key = base64.b64encode(enc_aes_key).decode()
                    secret_keys[c_id] = enc_aes_key

                act = 'create'
                info = {
                    'dict_id': str(self.dict_id),  # dict_id
                    'clients_id': data['clients_id'],
                    'secret_keys': secret_keys,
                    'payload': {}
                }

                self.dict_id = (self.dict_id[0], self.dict_id[1]+1)

            elif data['command'] == 'put':
                act = 'put'
                info = {
                    'dict_id': data['dict_id'],
                    'key': data['key'],
                    'value': data['value']
                }

            elif data['command'] == 'get':
                act = 'get'
                info = {
                    'dict_id': data['dict_id'],
                    'key': data['key']
                }

            new_entry = {
                'term': self.current_term,
                'act': act,
                'info': info,
                'user_addr': data['user_addr']
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
                # print(self.log.last_log_index)
                # print(self.log.entries)
                self.next_idx[data['src']] = self.log.last_log_index + 1

        while True:
            N = self.commit_idx + 1
            count = 1
            print(f"match_idx:{self.match_idx}")
            for peer in self.match_idx:
                if self.match_idx[peer] >= N:
                    count += 1
                if count > len(self.routes)//2:
                    self.commit_idx = N
                    print(f"[Log]: leader {self.node_id} commit entry {self.commit_idx}")
                    break
            else:
                break

    def generate_key(self):
        return os.urandom(16)

    def encrypt_message(self, message, key):
        cipher = AES.new(key, AES.MODE_CBC)
        iv = cipher.iv
        ciphertext = cipher.encrypt(pad(message.encode(), AES.block_size))
        return iv + ciphertext

    def decrypt_message(self, ciphertext, key):
        iv = ciphertext[:AES.block_size]
        ciphertext = ciphertext[AES.block_size:]
        cipher = AES.new(key, AES.MODE_CBC, iv=iv)
        plaintext = unpad(cipher.decrypt(ciphertext), AES.block_size)
        return plaintext.decode()

    
    def deal_clients(self, data):
        if data['command'] == 'create' or data['command'] == 'put' or data['command'] == 'get':
            data['term'] = self.current_term
            if self.state != 'leader':
                if self.leader_id:
                    data = json.dumps(data)
                    if self.routes[self.leader_id]['enable']:
                        print(f"follower {self.node_id} redirect data to leader {self.leader_id}")
                        self.send_udp_packet(data, self.routes[self.leader_id]['addr'], self.leader_id)
                        return None

        elif data['command'] == 'printDict':
            if str(data['dict_id']) not in self.dict:
                res = {
                    "act": "printDict",
                    "msg": f"[Node {self.node_id}] Dict {data['dict_id']} not found in current node! "
                }
                self.reply_to_user(data['user_addr'], res)
                return None

            find_dict = self.dict[str(data['dict_id'])]
            res = {
                "act": "printDict",
                "msg": f"[Node {self.node_id}] Dict {data['dict_id']} is {find_dict}"
            }
            self.reply_to_user(data['user_addr'], res)

            return None

        elif data['command'] == 'printAll':
            res = {
                "act": "printAll",
                "msg": f"[Node {self.node_id}] Dict storage {self.dict}"
            }
            self.reply_to_user(data['user_addr'], res)

            return None

        elif data['command'] == 'failLink':
            self.routes[data['dest']]['enable'] = False
            # need to response user using user_addr TODO
            return None
        elif data['command'] == 'fixLink':
            self.routes[data['dest']]['enable'] = True
            # need to response user using user_addr TODO
            return None
        else:
            print('kill precess')
            # need to response user using user_addr TODO

            self.save_ckpt()
            exit()

        return data

    def listen_for_udp(self):
        while not self.stop_udp_thread:
            try:
                data, addr = self.udp_sock.recvfrom(2048)
            except Exception as e:
                data, addr = None, None

            if data:
                # data = rsa.decrypt(data, self.private_key)
                data = data.decode("utf-8")
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
    user_id = sys.argv[1]
    recovery = int(sys.argv[2])
    config_path = "config.yaml"

    service = Service(user_id, config_path, recovery=recovery)


if __name__ == '__main__':
    run()

