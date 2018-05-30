import json
import logging

import re
from collections import Counter
from itertools import groupby
from operator import itemgetter

from job.util.Mongo import db
from job.util.Zk import zk_check
import numpy as np
from scipy.stats import entropy
from scipy.sparse import coo_matrix


@zk_check()
def dump_word_entropy() -> None:
    docs = db.stock_report.find({"word": {"$exists": True}},
                                {"_id": 0, "code": 1, "word": 1})
    logging.info("load complete")
    code_mapper = {code: idx for idx, code in
                   enumerate(db.stock_basics.distinct("code"))}
    word_code_list = []
    patt = re.compile("[0-9a-zA-Z\s]+")
    for d in docs:
        code = d["code"]
        # filter english and number
        word_code_list.extend([(w, code) for w in d["word"] if not patt.search(w)])
    count = [(w, (code_mapper[code], n)) for
             (w, code), n in Counter(word_code_list).items()]
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


if __name__ == '__main__':
    dump_word_entropy()
