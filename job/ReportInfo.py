from job.util.Zk import zk_check
from job.util.Mongo import db
import logging
from functools import partial
from itertools import chain
import datetime
from job.util.Spider import get_topic_from_sina, get_document_from_sina
from concurrent.futures import ThreadPoolExecutor


@zk_check()
def get_topic():
    stock = [d["code"] for d in db.stock_basics.find({}, {"code": 1, "_id": 0})]

    def fetch_function(check, code: str, page: int = 1) -> tuple:
        logging.info("start code {} page {}".format(code, page))
        links, max_page = get_topic_from_sina(code, page)
        if links:
            # check if the last url is in db
            if check is True and db.stock_report.find({"url": links[-1]["url"]}).count() == 0 and max_page > 1:
                max_page = max_page
            else:
                max_page = 1
            # update
            for link in links:
                link.setdefault("timestamp", datetime.datetime.now().strftime("%Y-%m-%d %X"))
                db.stock_report.update({"url": link["url"]}, {"$set": link}, True)
        return code, max_page

    with ThreadPoolExecutor(max_workers=6) as executor:
        result = executor.map(partial(fetch_function, True), stock)
        result = filter(lambda x: x[1] > 1, result)
        result = map(lambda x: [(x[0], i) for i in range(2, x[1]+1)], result)
        result = chain.from_iterable(result)
        executor.map(partial(fetch_function, False), *zip(*result))


@zk_check()
def get_document():
    urls = [d["url"] for d in db.stock_report.find({"content": {"$exists": False}},
                                                   {"_id": 0, "url": 1})]

    def fetch_function(url: str) -> str:
        data = get_document_from_sina(url)
        if data != {} and data.get("content") is None:
            data.setdefault("content", "--")
        data["content"] = data["content"].replace("\xa0\xa0\xa0", "")
        data.setdefault("timestamp", datetime.datetime.now().strftime("%Y-%m-%d %X"))
        # update
        db.stock_report.update({"url": url}, {"$set": data}, False)
        return url

    with ThreadPoolExecutor(max_workers=6) as executor:
        result = executor.map(fetch_function, urls)
        logging.info("get result count {}".format(len(list(result))))


if __name__ == '__main__':
    get_topic()
