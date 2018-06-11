import socketserver
import threading
import json
import os
import socket
from job.util import Zk
from job.util.BaiduNlp import BaiduNlp
from gensim.models import Doc2Vec
from job.util.Mongo import db

baidu_nlp = BaiduNlp()
model = Doc2Vec.load(os.path.abspath(__file__).replace("server" + os.sep + "ArticleServer.py",
                                                       "") + "data" + os.sep + "doc2vec_128.model")
code_name_mapper = {d["code"]: d["name"] for d in
                    db.stock_basics.find({}, {"_id": 0, "code": 1, "name": 1})}


class ArticleServer(socketserver.BaseRequestHandler):
    def handle(self):
        data = self.request.recv(1024)
        word = baidu_nlp.word(data.decode("gbk"))
        vector = model.infer_vector(word["word"])
        similar = model.docvecs.most_similar([vector], topn=10)
        similar = [(code, code_name_mapper.get(code, "UNK"), score) for
                   code, score in similar]
        response = bytes(json.dumps(similar), 'utf-8')
        self.request.sendall(response)


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    pass


@Zk.zk_check()
def article_server_run(ip_: str, port_: int) -> None:
    server = ThreadedTCPServer((ip_, port_), ArticleServer)
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()
    print("Server loop running in thread:", server_thread.name)

    input()
    server.shutdown()
    server.server_close()


def client(ip_: str, port_: int, message: str):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((ip_, port_))
        sock.sendall(bytes(message, 'gbk'))
        response = str(sock.recv(1024), 'utf-8')
        response = json.loads(response)
        print(response)


if __name__ == '__main__':
    s = """
    近日，国家印发《关于2018年光伏发电有关事项的通知》，文件规定，2018年在国家下文前各地不得安排需国家补贴的普通电站，被视为“行业急刹”。6月3日，11名光伏大佬致信新华社，建议给予已经合法批准开建的项目一定的缓冲期，希望政府相关部门多听听行业意见。
    """

    s = s.encode("gbk", "ignore").decode("gbk")
    ip = "localhost"
    port = 62345
    article_server_run(ip, port)
