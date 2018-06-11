import json
import logging
import random

import re
from collections import Counter
from itertools import groupby
from operator import itemgetter

from job.util.Mongo import db
from job.util.Zk import zk_check
import numpy as np
from scipy.stats import entropy
from scipy.sparse import coo_matrix
from sklearn.manifold import TSNE
from sklearn.linear_model import LogisticRegression
import matplotlib.pyplot as plt
from pylab import mpl

from gensim.models import Word2Vec, Doc2Vec
from gensim.models.doc2vec import TaggedDocument


# caculate the word entropy and save
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


# save key words what is filtered by entropy
def dump_keywords():
    entropy_ = json.load(open("../data/entropy.json", "r"))
    keywords = set(map(lambda x: x["word"], filter(lambda x: 0 < x["entropy"] < 4, entropy_)))
    keywords = list(keywords)
    json.dump(keywords, open("../data/keyword.json", "w"))


# save lexer
def dump_report():
    total = db.stock_report.find({"lexer": {"$exists": True, "$not": {"$size": 0}}},
                                 {"_id": 0, "code": 1, "lexer": 1, "word": 1}).count()
    logging.info("total {}".format(total))
    docs = db.stock_report.find({"lexer": {"$exists": True, "$not": {"$size": 0}}},
                                {"_id": 0, "code": 1, "lexer": 1, "word": 1})
    docs_list = []
    i = 0
    while True:
        i += 1
        try:
            d = next(docs)
            docs_list.append(d)
        except StopIteration as e:
            e.__traceback__
            break
        if i % 100 == 0:
            logging.info("{} % complete".format(i * 100 / total))
    json.dump(docs_list, open("../data/report.json", "w"))


# train word2vec
def train_word_to_vector(dim: int = 32):
    docs = json.load(open("../data/report.json", "r"))
    sentences = []
    for d in docs:
        sentences.append(d["lexer"])
    logging.info("load data complete")
    model = Word2Vec(sentences, size=dim, window=5, min_count=2, workers=4)
    model.save("../data/word2vec_{}.model".format(dim))
    logging.info("save model complete")


def keyword_bi():
    keywords = json.load(open("../data/keyword.json", "r"))
    keywords = set(keywords)
    model = Word2Vec.load("../data/word2vec_32.model")
    keywords = list(keywords.intersection(model.wv.vocab.keys()))
    matrix_A = []
    import random
    keywords_sample = random.choices(keywords, k=3000)
    for w in keywords_sample:
        matrix_A.append(model.wv[w])
    matrix_U, matrix_S, matrix_V = np.linalg.svd(matrix_A)
    print(matrix_S)
    print(sum(matrix_S[0:3]) / sum(matrix_S))


def test():
    keywords = json.load(open("../data/keyword.json", "r"))
    keywords = set(random.sample(keywords, 1000))
    model = Word2Vec.load("../data/word2vec_128.model")
    keywords = list(keywords.intersection(model.wv.vocab.keys()))
    ts = TSNE(2)
    matrix = [model.wv[w] for w in keywords]

    reduce_vectors = ts.fit_transform(matrix)
    print(reduce_vectors)
    mpl.rcParams['font.sans-serif'] = ['SimHei']
    for i in range(len(keywords)):
        x = reduce_vectors[i, 0]
        y = reduce_vectors[i, 1]
        plt.plot(x, y,
                 marker="o")
        plt.text(x, y, keywords[i])
    plt.show()


def train_doc_to_vector(dim: int = 32):
    docs = json.load(open("../data/report.json", "r"))
    logging.info("load data complete")
    sentences = []
    for d in docs:
        sentences.append(TaggedDocument(d["lexer"], [d["code"]]))
    logging.info("make sentences complete")
    del docs
    logging.info("del docs complete")
    model = Doc2Vec(min_count=1, window=20, size=dim,
                    sample=1e-3, negative=5, dm=1, workers=4)
    model.build_vocab(sentences)
    logging.info("build vocab complete")
    model.train(sentences,
                total_examples=model.corpus_count,
                epochs=50
                )
    logging.info("train complete")
    model.save("../data/doc2vec_{}.model".format(dim))
    logging.info("save complete")


if __name__ == '__main__':
    train_doc_to_vector(128)
