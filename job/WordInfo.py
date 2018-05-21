from job import db, zk_check
from job.BaiduNlp import BaiduNlp
from job.ProgressBar import ProgressBar
from scipy.stats import entropy
from scipy.sparse import coo_matrix
from collections import Counter
from itertools import groupby, chain
from operator import itemgetter
import numpy as np
import logging
import re
import json


@zk_check()
def get_report_word() -> None:
    docs = [d for d in db.stock_report.find({"word": {"$exists": False},
                                             "content": {"$exists": True}},
                                            {"_id": 0, "url": 1, "title": 1, "content": 1})]
    baidu_nlp = BaiduNlp()
    bar = ProgressBar(total=len(docs))
    for d in docs:
        bar.move()
        try:
            word = list(baidu_nlp.word(d.get("title", "") + " " + d["content"]))
        except UnicodeEncodeError as e:
            bar.log(e)
            continue
        db.stock_report.update({"url": d["url"]}, {"$set": {"word": word}})
        bar.log("url {} success.".format(d["url"]))


@zk_check()
def dump_word_entropy() -> None:
    docs = [d for d in db.stock_report.find({"word": {"$exists": True}},
                                            {"_id": 0, "code": 1, "word": 1})]
    logging.info("load complete")
    code_mapper = {code: idx for idx, code in
                   enumerate(db.stock_basics.distinct("code"))}
    code_word = []
    patt = re.compile("[0-9a-zA-Z\s]+")
    for d in docs:
        code = d["code"]
        # filter english
        code_word.extend([(w, code) for w in d["word"] if not patt.search(w)])
    count = [(word, (code_mapper[code], n)) for
             (word, code), n in Counter(code_word).items()]
    # noinspection PyTypeChecker
    count.sort(key=itemgetter(0))
    row, col, val = [], [], []
    i = 0
    word, word_topic, word_n = [], [], []
    for k, v in groupby(count, key=itemgetter(0)):
        address = list(map(itemgetter(1), v))
        col_ = np.ones(len(address)) * i
        row_, val_ = zip(*address)
        col.extend(col_)
        row.extend(row_)
        val.extend(val_)
        word.append(k)
        word_topic.append(len(val_))
        word_n.append(sum(val_))
        i += 1
    logging.info("data complete")
    coo = coo_matrix((val, (row, col)), shape=(len(code_mapper), len(word)))
    logging.info("coo complete")
    word_entropy = entropy(coo.toarray())
    logging.info("entropy complete")
    data = []
    for i in range(len(word)):
        data.append({"word": word[i],
                     "entropy": word_entropy[i],
                     "topic_n": word_topic[i],
                     "n": word_n[i]})
    logging.info("save complete")
    json.dump(data, open("../data/entropy.json", "w"))
    # db.word_entropy.drop()
    # logging.info("drop complete")
    # db.word_entropy.insert(data)
    # logging.info("job complete")


@zk_check()
def commit_entropy_file():
    data = json.load(open("../data/entropy.json", "r"))
    db.word_entropy.drop()
    logging.info("drop complete")
    db.word_entropy.insert(data)
    logging.info("job complete")


def get_keyword() -> None:
    docs = [d for d in db.stock_report.find({"word": {"$exists": True},
                                             "keyword": {"$exists": False}},
                                            {"_id": 0, "url": 1, "code": 1, "word": 1}).limit(2000)]
    keyword = [d for d in db.word_entropy.find({"topic_n": {"$gt": 3},
                                                "entropy": {"$lt": 3}},
                                               {"_id": 0, "word": 1})]
    keyword = set([d["word"] for d in keyword])
    logging.info("keyword count {}".format(len(keyword)))
    bar = ProgressBar(total=len(docs))
    for d in docs:
        bar.move()
        keyword_ = list(keyword.intersection(d["word"]))
        db.stock_report.update({"url": d["url"]}, {"$set": {"keyword": keyword_}}, False)
        bar.log("code {} keyword {}".format(d["code"], keyword_))


if __name__ == '__main__':
    # dump_word_entropy()
    # commit_entropy_file()
    get_keyword()
