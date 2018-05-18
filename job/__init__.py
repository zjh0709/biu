import pymongo
import logging
from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S')

client = pymongo.MongoClient(host="master", port=17585)
db = client.get_database("biu")


def zk_check(func, *args, **kwargs):
    try:
        zk = KazooClient(hosts="master:2181")
        zk.start()
    except KazooTimeoutError as e:
        logging.error(e)
        exit(0)
    zk_node = "/biu/" + func.__name__
    if zk.exists(zk_node):
        logging.warning("last {} is still running".format(func.__name__))
        return None
    zk.create(path=zk_node, value=b"running", ephemeral=True, makepath=True)
    func(*args, **kwargs)
    zk.delete(zk_node)
    zk.stop()


index_mapper = {
    "000001": "sh000001",
    "000002": "sh000002",
    "000003": "sh000003",
    "000008": "sh000008",
    "000009": "sh000009",
    "000010": "sh000010",
    "000011": "sh000011",
    "000012": "sh000012",
    "000016": "sh000016",
    "000017": "sh000017",
    "000300": "sh000300",
    "000905": "sh000905",
    "399001": "sz399001",
    "399002": "sz399002",
    "399003": "sz399003",
    "399004": "sz399004",
    "399005": "sz399005",
    "399006": "sz399006",
    "399008": "sz399008",
    "399100": "sz399100",
    "399101": "sz399101",
    "399106": "sz399106",
    "399107": "sz399107",
    "399108": "sz399108",
    "399333": "sz399333",
    "399606": "sz399606"
}

if __name__ == "__main__":
    print(client.biu.basics.count())
