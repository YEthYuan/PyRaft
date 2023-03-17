import socket
import json


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


def run():
    user_addr = ('127.0.0.1', 8010)
    recv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    recv_sock.bind(user_addr)

    listen_for_responses(recv_sock)


if __name__ == "__main__":
    run()