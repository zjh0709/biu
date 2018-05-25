import logging
from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError
from ..config import ZK_HOST, ZK_ROOT


def zk_check():
    def wrape(func):
        def todo(*args, **kwargs):
            try:
                zk = KazooClient(hosts=ZK_HOST)
                zk.start()
            except KazooTimeoutError as e:
                logging.error(e)
                return None
            zk_node = ZK_ROOT + func.__name__
            logging.info("zk_node is {}".format(zk_node))

            if zk.exists(zk_node):
                logging.warning("last {} is still running".format(func.__name__))
                zk.stop()
                return None
            else:
                zk.create(path=zk_node, value=b"running", ephemeral=True, makepath=True)
                rs = func(*args, **kwargs)
                zk.delete(zk_node)
                zk.stop()
                return rs

        return todo

    return wrape
