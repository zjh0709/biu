import socket
import random
import logging
from itertools import product
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S')


def make_ip(num: int, a: int, b: int, c_start: int, c_end: int, d_start: int, d_end: int) -> list:
    ip_pool_ = []
    for _ in range(num):
        c = random.randint(c_start, c_end)
        d = random.randint(d_start, d_end)
        ip_pool_.append("{}.{}.{}.{}".format(a, b, c, d))
    return ip_pool_


def scan(ip: str, port: int):
    global i, pool_count
    i += 1
    if i % 1000 == 0:
        logging.info("process {} %".format(i*100 / pool_count))
    try:
        result = TCP_sock.connect_ex((ip, port))
        # logging.info("ip: {}, port: {} result {}".format(ip, port, result))
        if result == 0:
            logging.info("ip: {}, port: {}".format(ip, port))
    except Exception as e:
        logging.warning(e)


if __name__ == '__main__':
    TCP_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    TCP_sock.settimeout(5)
    ip_pool = make_ip(num=1000000, a=58, b=33, c_start=21, c_end=255, d_start=0, d_end=255)
    port_pool = [9200, 80]
    pool = list(product(ip_pool, port_pool))
    pool_count = len(pool)
    logging.info("ip,port count {}".format(pool_count))
    i = 0

    with ThreadPoolExecutor(max_workers=30) as executor:
        executor.map(scan, *zip(*pool))
