import argparse
import json
import threading

import yaml
import socket

parser = argparse.ArgumentParser(description='user commands.')
subparsers = parser.add_subparsers(dest='command')

# create command
create_parser = subparsers.add_parser('create')
create_parser.add_argument('client_ids', nargs='+', help='list of client ids')

# put command
put_parser = subparsers.add_parser('put')
put_parser.add_argument('dict_id', type=str, help='dictionary id')
put_parser.add_argument('key', help='key')
put_parser.add_argument('value', help='value')

# get command
get_parser = subparsers.add_parser('get')
get_parser.add_argument('dict_id', type=str, help='dictionary id')
get_parser.add_argument('key', help='key')

# printDict command
print_dict_parser = subparsers.add_parser('printDict')
print_dict_parser.add_argument('dict_id', type=str, help='dictionary id')

# printAll command
subparsers.add_parser('printAll')

# failLink command
fail_link_parser = subparsers.add_parser('failLink')
fail_link_parser.add_argument('dest', help='destination process')

# fixLink command
fix_link_parser = subparsers.add_parser('fixLink')
fix_link_parser.add_argument('dest', help='destination process')

# failProcess command
subparsers.add_parser('failProcess')


def listen_for_responses(recv_sock):
    while True:
        data, addr = recv_sock.recvfrom(2048)
        response = json.loads(data.decode())
        # print(f"Response from {addr}: {response}")
        if response['act'] == "create":
            print(response['msg'])
        elif response['act'] == "put":
            print(response['msg'])
        elif response['act'] == "get":
            print(response['msg'])
        else:
            print(response['msg'])


def str2tuple(input_string: str) -> tuple:
    input_list = input_string.split(",")
    input_tuple = tuple(input_list)
    return input_tuple


def run():
    user_id = int(input("Please input user_id:"))
    config_path = "config.yaml"
    with open(config_path, 'r') as file:
        config = yaml.load(file, Loader=yaml.FullLoader)

    for client in config['clients']:
        if user_id == client['nodeId']:
            src_addr = (client['ip'], client['port'])

    user_addr = ('127.0.0.1', 8010)
    recv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    recv_sock.bind(user_addr)

    args = parser.parse_args()

    if args.command == 'create':
        request = {
            'type': 'ClientRequest',
            'user_addr': user_addr,
            'command': args.command,
            'clients_id': args.client_ids
        }
    elif args.command == 'put':
        request = {
            'type': 'ClientRequest',
            'user_addr': user_addr,
            'command': args.command,
            'dict_id': str2tuple(args.dict_id),
            'client_id': user_id,
            'key': args.key,
            'value': args.value
        }
    elif args.command == 'get':
        request = {
            'type': 'ClientRequest',
            'user_addr': user_addr,
            'command': args.command,
            'dict_id': str2tuple(args.dict_id),
            'client_id': user_id,
            'key': args.key
        }
    elif args.command == 'printDict':
        request = {
            'type': 'ClientRequest',
            'user_addr': user_addr,
            'command': args.command,
            'dict_id': str2tuple(args.dict_id)
        }
    elif args.command == 'fixLink' or args.command == 'failLink':
        request = {
            'type': 'ClientRequest',
            'user_addr': user_addr,
            'command': args.command,
            'dest': args.dest
        }
    else:
        request = {
            'type': 'ClientRequest',
            'user_addr': user_addr,
            'command': args.command
        }

    request = json.dumps(request)
    send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    send_sock.sendto(request.encode(), src_addr)

    if args.command == 'failLink' or args.command == 'fixLink':
        request['dest'] = user_id
        for client in config['clients']:
            if args.dest == client['nodeId']:
                dest_addr = (client['ip'], client['port'])
        send_sock.sendto(request.encode(), dest_addr)

    listener_thread = threading.Thread(target=listen_for_responses, args=(recv_sock,))
    listener_thread.daemon = True
    listener_thread.start()

    listener_thread.join()




if __name__ == "__main__":
    run()