import argparse
import time
from _thread import *
import asyncio
import logging
import socket
import json
import random
import file_transfer
import sys
import socketserver
import os
import socket
import json
import re

logging.basicConfig(format='%(asctime)s %(lineno)d %(levelname)s:%(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)

HOST = '0.0.0.0'
PORT = 8080
PEER_SRV_HOST = "127.0.0.1"
PEER_SRV_PORT = 9009
DEFAULT_DEST = "./"

file_mapping_table = {}

def get_file_location(fname):
    """ Get the location of the published file `fname`. """
    if fname not in file_mapping_table:
        return None

    file_loc = file_mapping_table[fname]
    return os.path.exists(file_loc) and file_loc


class UnknownCommandError(Exception):
    def __init__(self, cmd):
        self.cmd = cmd

    def __str__(self):
        return f'unknown command: {self.cmd}'

def parse_cmd(cmd_str):
    cmd_args = cmd_str.split()
    cmd = cmd_args[0]
    if cmd not in ('publish', 'fetch'):
        raise UnknownCommandError(cmd_str)
    if cmd == 'publish':
        lname, fname = cmd_args[1], cmd_args[2]
        return cmd, (lname, fname)

    return cmd, cmd_args[1]

def print_usage():
    print('usage: publish <lname> <fname> | fetch <fname>')
     
def handle_publish(lname, fname, conn):
    """TODO: publish file to server"""

    try:
        if fname in file_mapping_table:
            if file_mapping_table[fname] == lname:
                print('File has been published before.')
            elif file_mapping_table[fname] != lname:
                print(f'File named {fname} already exists. Update present local path of {fname} to new local path {lname}.')
                file_mapping_table[fname] = lname
        elif lname in file_mapping_table:
            for file_name, local_path in file_mapping_table.items():
                if local_path == lname and file_name != fname:
                    print(f'Local path {lname} belongs to a file with different name than {fname}. Update name of file to {fname}.')
                    del file_mapping_table[fname]
                    file_mapping_table[fname] = lname
                    break
        else:
            file_mapping_table[fname] = lname
        print(file_mapping_table)
    
        message = json.dumps({'publish' : fname, 'seeding_port': PEER_SRV_PORT})
        try:
            conn.sendall(message.encode('utf-8'))
        except socket.error as e:
            logger.error(e)

    except Exception as e:
        raise e


def get_peers_from_srv(fname: str, host: str, port: int):
    """ Retrieve all peers that contain 'fname' in their repositories.

    Args:
        fname(str): name of the file being requested 
        host(str): ipv4 address of the server
        port(str): port on which the server listens 

    Returns:
        list: A list of string representing peers in the following format: "{host}:{port}"
    """

    fetch_req = file_transfer.create_fetch_request(fname)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((host, port))
        sock.send(fetch_req)
        raw_resp = sock.recv(1024)
        peers = json.loads(raw_resp)
        return peers

def choose_peer(fname, peers):
    """ Choose an appropriate peer from a list of peers to fetch the file `fname`
        by using random selection policy.

    Args:
        fname (str): published file
        peers (iter): host-port pair representing the peer containing `fname`.

    Returns:
        An item in peers.
    """

    # Pick a random peer
    while len(peers) > 0:
        peer = random.choice(peers)
        logger.debug(f"In choose_peer(): Choose peer: {peer}")
        try:
            with socket.create_connection(peer) as sock:
                sock.send(file_transfer.create_check_request(fname))
                data = sock.recv(1024).decode()
                if data == "EXIST":
                    return peer
            peers.remove(peer)
        except ConnectionRefusedError as e:
            peers.remove(peer)
    return None

def handle_fetch_cmd(fname):
    """TODO: send request to server
        -> server return data consist of peer host and port
        -> send request retrieve file from peer
        -> peer send back file (using ftp)
    """

    logger.info(f"Fetching file '{fname}'...")
    peers = get_peers_from_srv(fname, server_host, server_port)
    logger.debug(f"Peers containing '{fname}': {peers}")

    if peers == []:
        print(f"No file named '{fname}' was published.")
        return

    peer = choose_peer(fname, peers)
    if not peer:
        # There is no peer in the retrieved list currenly containing the file
        print(f"No peer in the found peers currently contain the file {fname}.")
        return

    p_host, p_port = peer
    file_dir = f"{DEFAULT_DEST}/{fname}"
    if os.path.exists(file_dir):
        # Create new name for the downloaded file
        # The new name has the following pattern: "<fname> (<index>).ext"
        froot, ext = os.path.splitext(fname)
        reg = "^{}( \(\d+\))?{}{}$".format(froot, '.' if ext else '', ext)
        dest = DEFAULT_DEST

        # Find maximum index among the names: [<fname> (<index 1>).ext, <fname> (<index 2>).ext, .etc] 
        matches = filter(lambda match: match, [re.fullmatch(reg, name) for name in os.listdir(dest) if os.path.isfile(os.path.join(dest, name))])
        max_idx = max(map(lambda match: 0 if not match.group(1) else int(re.split("\(|\)", match.group(1))[1]), matches))

        file_dir = f"{DEFAULT_DEST}/{froot} ({max_idx + 1}){ext}"

        print(f"Detect a file named {fname} in the default destination.")
        print(f"The downloaded file will be renamed as '{froot} ({max_idx + 1}){ext}'.")

    try:
        file_transfer.fetch_file(fname=fname, host=p_host, port=p_port, file_dir=file_dir)
        print(f"Successfully fetched '{fname}'. Destination: '{os.path.abspath(file_dir)}'")
    except OSError as e:
        print(f"An error has occured while fetching the file. Please try again.")
        logger.debug(str(e))

def shell_command_handler(conn):
    while True:
        print('> ', end='')
        try:
            cmd, args = parse_cmd(input())
            if cmd == 'publish':
                handle_publish(args[0], args[1], conn)
            elif cmd == 'fetch':
                handle_fetch_cmd(args)
        except UnknownCommandError as e:
            print_usage()
        except IndexError:
            print_usage()

def handle_request_from_server(conn):
    while True:
        message = conn.recv(4096)
        if message == b'':
            sys.exit(0)
        elif message == b'ping':
            conn.send(b'Alive')
        elif message == b'discover':
            # folder_path = "C:\shared_folder"
            # if os.path.exists(folder_path) and os.path.isdir(folder_path):
                # file_list = os.listdir(folder_path)
            file_list = []
            for fname, lname in file_mapping_table.items():
                if os.path.isfile(lname):
                    file_list.append(fname)

            files_data = json.dumps({'files': file_list})
            try:
                conn.sendall(files_data.encode('utf-8'))
                print("Send data successfully")
                print(f"Data to be sent: {files_data}")
            except socket.error as e:
                    logger.error(e)
            # else:
                # print(f"No folder path {folder_path}")

class PeerRequestHandler(socketserver.BaseRequestHandler):
    def is_valid_request(self, raw_req: bytes):
        """ Check if the raw request is valid. """
        valid_fields = ['op', 'fname']
        parsed_rq : dict = json.loads(raw_req)
        for field in parsed_rq.keys():
            if field not in valid_fields:
                return False
        
        return parsed_rq['op'] in ['fetch', 'check'] and isinstance(parsed_rq['fname'], str)

    def handle(self):
        """ Handle incoming requests from other peers. """
        raw_req = self.request.recv(1024)
        logger.debug(f"{self.client_address} connected. message={raw_req}")
        if not self.is_valid_request(raw_req):
            self.request.send("INVALID REQUEST".encode())

        parsed_req = json.loads(raw_req)

        if parsed_req["op"] == "fetch":
            fname = parsed_req['fname']
            f_loc = get_file_location(fname)
            with open(f_loc, "rb") as f:
                chunk = f.read(8192)
                while chunk:
                    self.request.send(chunk)
                    chunk = f.read(8192)
        elif parsed_req["op"] == "check":
            fname = parsed_req['fname']
            logger.debug(f"location of '{fname}': {get_file_location(fname)}")
            if get_file_location(fname):
                self.request.send("EXIST".encode())
            else:
                self.request.send("NOT EXIST".encode())

def handle_request_from_peer(peer_srv: socketserver.TCPServer):
    try:
        peer_srv.serve_forever()
    finally:
        peer_srv.server_close()

if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-H', '--server-host')
    arg_parser.add_argument('-P', '--server-port')
    arg_parser.add_argument('--peer-srv-host')
    arg_parser.add_argument('--peer-srv-port')
    arg_parser.add_argument('--default-dest')
    args = arg_parser.parse_args()
    server_host = args.server_host or 'localhost'
    server_port = int(args.server_port or '8080')

    # if the seeding server's port is not specified, tell Python to choose a random unused port
    PEER_SRV_HOST = (args.peer_srv_host or "")
    PEER_SRV_PORT = int(args.peer_srv_port or 0)    
    DEFAULT_DEST = args.default_dest or "./"

    try:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.connect((server_host, server_port))

        start_new_thread(handle_request_from_server, (server,))
        peer_srv = socketserver.ThreadingTCPServer((PEER_SRV_HOST, PEER_SRV_PORT), PeerRequestHandler)
        logger.debug(f"Start listening to other peers at address {(PEER_SRV_HOST, PEER_SRV_PORT)}.")

        # Create new PyThread to handle request from other peers
        start_new_thread(handle_request_from_peer, (peer_srv,))

        shell_command_handler(server)
    finally:
        server.close()
