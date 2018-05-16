import os
import sys
import pymongo

with open(os.path.abspath("config.properties")) as f:
    # noinspection PyTypeChecker
    config = dict([line.strip().split("=", 1) for
                   line in f.readlines() if "=" in line])
client = pymongo.MongoClient(host=config.get("mongo_host"),
                             port=int(config.get("mongo_port")))
db = client.get_database(config.get("mongo_db"))


class ProgressBar(object):
    def __init__(self, count=0, total=0, width=50):
        self.count = count
        self.total = total
        self.width = width

    def move(self):
        self.count += 1

    def log(self, s):
        sys.stdout.write(" " * (self.width + 9) + "\r")
        sys.stdout.flush()
        print(s)
        progress = int(self.width * self.count / self.total)
        sys.stdout.write("{0:3}/{1:3}: ".format(self.count, self.total))
        sys.stdout.write("#" * progress + "-" * (self.width - progress) + "\r")
        if progress == self.width:
            sys.stdout.write("\n")
        sys.stdout.flush()


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
