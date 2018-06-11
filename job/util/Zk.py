import logging
from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError
from ..config import ZK_HOST, ZK_ROOT


def zk_client():
    try:
        zk = KazooClient(hosts=ZK_HOST)
        zk.start()
        return zk
    except KazooTimeoutError as e:
        logging.error(e)
        exit("zk connect error")


def zk_check():
    def wrap(func):
        def todo(*args, **kwargs):
            zk = zk_client()
            zk_node = ZK_ROOT + func.__name__
            logging.info("zk_node is {}".format(zk_node))

            if zk.exists(zk_node):
                logging.warning("last {} is still running".format(func.__name__))
                zk.stop()
                return None
            else:
                zk.create(path=zk_node, value=b"running", ephemeral=True, makepath=True)
                rs = func(*args, **kwargs)
                logging.info("{} complete".format(func.__name__))
                zk.delete(zk_node)
                zk.stop()
                return rs

        return todo

    return wrap
