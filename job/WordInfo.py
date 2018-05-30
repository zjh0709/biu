import logging
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import json
import os

from job.util.BaiduNlp import BaiduNlp
from job.util.Mongo import db
from job.util.ProgressBar import ProgressBar
from job.util.Zk import zk_check


def explode(nlp: BaiduNlp, collection: str, doc: dict):
    url = doc.get("url")
    title = doc.get("title", "").encode("gbk", "ignore").decode("gbk")
    content = doc.get("content", "").encode("gbk", "ignore").decode("gbk")
    try:
        data = nlp.word(title + " " + content)
    except Exception as e:
        logging.warning(e)
    db.get_collection(collection).update({"url": url}, {"$set": data})
    logging.info("{} lexer {} word {}.".format(title,
                                               len(data.get("lexer", [])),
                                               len(data.get("word", []))))
    return url


@zk_check()
def get_report_word(num: int = 1000) -> None:
    docs = db.stock_report.find({"$or": [{"word": {"$exists": False}}, {"lexer": {"$exists": False}}],
                                 "content": {"$exists": True}},
                                {"_id": 0, "url": 1, "title": 1, "content": 1}).limit(num)
    baidu_nlp = BaiduNlp()
    with ThreadPoolExecutor(max_workers=3) as executor:
        result = executor.map(partial(explode, baidu_nlp, "stock_report"), docs)
        logging.info("get result count {}".format(len(list(result))))


@zk_check()
def get_news_word(num: int = 1000) -> None:
    docs = db.break_news.find({"word": {"$exists": False},
                               "content": {"$exists": True}},
                              {"_id": 0, "url": 1, "title": 1, "content": 1}).limit(num)
    baidu_nlp = BaiduNlp()
    with ThreadPoolExecutor(max_workers=3) as executor:
        result = executor.map(partial(explode, baidu_nlp, "break_news"), docs)
        logging.info("get result count {}".format(len(list(result))))


@zk_check()
def get_report_keyword(num: int = 1000) -> None:
    entropy_file = os.path.abspath(__file__).replace("job" + os.sep + "WordInfo.py",
                                                     "") + "data" + os.sep + "entropy.json"
    entropy = json.load(open(entropy_file, "r"))
    keyword = set(map(lambda x: x["word"], filter(lambda x: 0 < x["entropy"] < 4, entropy)))
    docs = list(db.stock_report.find({"word": {"$exists": True},
                                      "keyword": {"$exists": False}},
                                     {"_id": 0, "url": 1, "code": 1, "word": 1}).limit(num))
    logging.info("keyword count {}".format(len(keyword)))
    bar = ProgressBar(total=len(docs))
    for d in docs:
        bar.move()
        keyword_ = list(keyword.intersection(d["word"]))
        db.stock_report.update({"url": d["url"]}, {"$set": {"keyword": keyword_}}, False)
        bar.log("code {} keyword {}".format(d["code"], keyword_))


@zk_check()
def get_news_keyword(num: int = 1000) -> None:
    entropy_file = os.path.abspath(__file__).replace("job" + os.sep + "WordInfo.py",
                                                     "") + "data" + os.sep + "entropy.json"
    entropy = json.load(open(entropy_file, "r"))
    keyword = set(map(lambda x: x["word"], filter(lambda x: 0 < x["entropy"] < 4, entropy)))
    docs = list(db.break_news.find({"word": {"$exists": True},
                                    "keyword": {"$exists": False}},
                                   {"_id": 0, "url": 1, "code": 1, "word": 1}).limit(num))
    logging.info("keyword count {}".format(len(keyword)))
    bar = ProgressBar(total=len(docs))
    for d in docs:
        bar.move()
        keyword_ = list(keyword.intersection(d["word"]))
        db.break_news.update({"url": d["url"]}, {"$set": {"keyword": keyword_}}, False)
        bar.log("code {} keyword {}".format(d["code"], keyword_))


if __name__ == '__main__':
    get_report_word(20)
