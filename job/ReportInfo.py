from job.util.Zk import zk_check
from job.util.Mongo import db
import logging
from functools import partial
from job.util.Spider import get_topic_from_sina, get_document_from_sina
from concurrent.futures import ThreadPoolExecutor


@zk_check()
def get_topic():
    stock = [d["code"] for d in db.stock_basics.find({}, {"code": 1, "_id": 0}).limit(10)]
    step2_pool = []

    def fetch_function(check, code: str, page: int = 1) -> tuple:
        links, max_page = get_topic_from_sina(code, page)
        if links:
            # check if the last url is in db
            if check is True and db.stock_report.find({"url": links[-1]["url"]}).count() == 0 and max_page > 1:
                step2_pool.extend([(code, i) for i in range(1, max_page + 1)])
            # update
            for link in links:
                db.stock_report.update({"url": link["url"]}, {"$set": link}, True)
                logging.info(link["title"])
        return code, page

    with ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(partial(fetch_function, True), stock)
    logging.info("step 1 complete.")
    if step2_pool:
        logging.info(step2_pool)
        with ThreadPoolExecutor(max_workers=3) as executor:
            executor.map(partial(fetch_function, False), *zip(*step2_pool))


@zk_check()
def get_document():
    urls = [d["url"] for d in db.stock_report.find({"content": {"$exists": False}},
                                                   {"_id": 0, "url": 1}).limit(10)]

    def fetch_function(url: str) -> str:
        data = get_document_from_sina(url)
        if data != {} and data.get("content") is None:
            data.setdefault("content", "--")
        data["content"] = data["content"].replace("\xa0\xa0\xa0", "")
        # update
        db.stock_report.update({"url": url}, {"$set": data}, False)
        return data

    with ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(fetch_function, urls)


if __name__ == '__main__':
    get_document()
