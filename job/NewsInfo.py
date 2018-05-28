from job.util.Zk import zk_check
from job.util.Mongo import db
import tushare as ts
import time
import logging
from concurrent.futures import ThreadPoolExecutor


@zk_check()
def get_news_url(num: int = 1000) -> None:
    df = ts.get_latest_news(top=num, show_content=False)
    df['timestamp'] = int(time.time())
    data = df.to_dict(orient="records")

    def update(d):
        db.break_news.update({"url": d["url"]}, {"$set": d}, True)
        logging.info(d["url"])

    with ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(update, data)


@zk_check()
def get_news_content(num: int = 1000) -> None:
    data = [d["url"] for d in db.break_news.find({"content": {"$exists": False}}).limit(num)]

    def update(d):
        content = ts.latest_content(d)
        content = content if content is not None else "--"
        db.break_news.update({"url": d}, {"$set": {"content": content}}, False)
        logging.info(d)

    with ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(update, data)


if __name__ == "__main__":
    get_news_url(500)
