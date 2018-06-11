import socketserver
import threading
import socket
from job.util import Zk


class TestServer(socketserver.BaseRequestHandler):
    def handle(self):
        data = str(self.request.recv(1024), 'ascii')
        cur_thread = threading.current_thread()
        response = bytes("{}: {}".format(cur_thread.name, data), 'ascii')
        self.request.sendall(response)


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    pass


def client(ip_: str, port_: int, message: str):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((ip_, port_))
        sock.sendall(bytes(message, 'ascii'))
        response = str(sock.recv(1024), 'ascii')
        print("Received: {}".format(response))


@Zk.zk_check()
def server_run(ip_: str, port_: int) -> None:
    server = ThreadedTCPServer((ip_, port_), TestServer)
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()
    print("Server loop running in thread:", server_thread.name)

    input()
    server.shutdown()
    server.server_close()


if __name__ == '__main__':
    ip = "localhost"
    port = 62345
    server_run(ip, port)
