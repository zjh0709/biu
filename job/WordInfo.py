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
import random


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
            word = list(baidu_nlp.word(d.get("title", "") + d["content"]))
        except UnicodeEncodeError as e:
            bar.log(e)
            continue
        db.stock_report.update({"url": d["url"]}, {"$set": {"word": word}})
        bar.log("url {} success.".format(d["url"]))


@zk_check()
def get_word_entropy() -> None:
    docs = [d for d in db.stock_report.find({"word": {"$exists": True}},
                                            {"_id": 0, "code": 1, "word": 1})]
    logging.info("load complete")
    code_mapper = {code: idx for idx, code in
                   enumerate(set([d["code"] for d in docs]))}
    code_word = []
    for d in docs:
        code = d["code"]
        code_word.extend([(w, code) for w in d["word"]])
    count = [(word, (code_mapper[code], n)) for
             (word, code), n in Counter(code_word).items()]
    # noinspection PyTypeChecker
    count.sort(key=itemgetter(0))
    for k, v in groupby(count, key=itemgetter(0)):
        address = list(map(itemgetter(1), v))
        row = np.zeros(len(address))
        col, val = zip(*address)
        coo = coo_matrix((val, (row, col)), shape=(1, len(code_mapper))).toarray()[0]
        logging.info("{} {}".format(k, entropy(coo)))
        db.word.update({"word": k}, {"$set": {"entropy": entropy(coo)}}, True)


def get_word_entropy_iter():
    total = db.stock_report.count({"word": {"$exists": True}})
    code_mapper = {code: idx for idx, code in
                   enumerate(db.stock_report.distinct("code"))}
    skip_n = random.randint(0, total - 10)
    word = [d["word"] for d in
            db.stock_report.find({"word": {"$exists": True}},
                                 {"_id": 0, "word": 1}).skip(skip_n).limit(10)]
    word = set(chain.from_iterable(word))
    bar = ProgressBar(total=len(word))
    for k in word:
        bar.move()
        if db.word.count({"word": k, "entropy": {"$exists": True}}) > 0:
            bar.log("{} exists".format(k))
            continue
        current = [d["code"] for d in
                   db.stock_report.find({"word": k}, {"_id": 0, "code": 1})]
        address = [(code_mapper[code], n) for
                   code, n in Counter(current).items()]
        row = np.zeros(len(address))
        col, val = zip(*address)
        coo = coo_matrix((val, (row, col)), shape=(1, len(code_mapper))).toarray()[0]
        bar.log("{} {}".format(k, entropy(coo)))
        db.word.update({"word": k}, {"$set": {"entropy": entropy(coo)}}, True)


if __name__ == '__main__':
    get_word_entropy_iter()
