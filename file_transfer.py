import json
import socket

def create_fetch_request(fname) -> bytes:
    req = {
        'op': 'fetch',
        'fname': fname
    }

    return json.dumps(req).encode()

def create_check_request(fname) -> bytes:
    req = {
        'op': 'check',
        'fname': fname
    }

    return json.dumps(req).encode()

def is_fetch_request(raw_req: bytes):
    """ Check if the raw request is a `fetch` request. """
    fields = ['op', 'fname']
    parsed_rq : dict = json.loads(raw_req)
    for field in fields:
        if field not in parsed_rq.keys():
            return False
        
    return parsed_rq['op'] == 'fetch' and isinstance(parsed_rq['fname'], str)

def fetch_file(fname, host, port, file_dir):
    """ Fetch file `fname` from peer `(host, port)`.

    Args:
        fname (str): name of the file being request in peer's repository.
        host (str): ipv4 address of the peer whose file is requested.
        port (int): port number of the peer.
        file_dir (str): the place to store the file being downloaded.
    """

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((host, port))
        file_rq = create_fetch_request(fname)

        sock.send(file_rq)

        with open(file_dir, 'wb') as f:
            data = sock.recv(8192)
            while data:
                f.write(data)
                data = sock.recv(8192)