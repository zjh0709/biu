from job import client, db
from job.Worker import Worker
import time
import datetime
import logging
import requests
from bs4 import BeautifulSoup
import re
import multiprocessing


def get_topic():
    stocks = [d["code"] for d in db.stock_basics.find({}, {"code": 1, "_id": 0})]
    workers = [TopicWorker(address=client.address, db_name=db.name, worker_name=worker_name) for
               worker_name in ["topic_worker1", "topic_worker2", "topic_worker3"]]
    # distribute stock
    worker_no = 0
    worker_count = len(workers)
    for stock in stocks:
        workers[worker_no].job_append({"code": stock, "page": 1})
        worker_no = (worker_no + 1) % worker_count
    # multiprocessing
    processes = []
    for worker in workers:
        processes.append(multiprocessing.Process(target=worker.job_run,
                                                 name=worker.worker_name,
                                                 args=()))
    for p in processes:
        p.daemon = True
        p.start()
        logging.info("p.pid:{} p.name:{} p.is_alive {}".format(p.pid, p.name, p.is_alive()))
    processes = [p for p in processes if p.is_alive()]
    for p in processes:
        p.join()
    logging.info("get topic Complete.")


def get_document():
    urls = [d["url"] for d in db.stock_report.find({"content": {"$exists": False}},
                                                   {"_id": 0, "url": 1})]
    workers = [DocumentWorker(address=client.address, db_name=db.name, worker_name=worker_name) for
               worker_name in ["document_worker1", "document_worker2", "document_worker3"]]
    # distribute stock
    worker_no = 0
    worker_count = len(workers)
    for url in urls:
        workers[worker_no].job_append(url)
        worker_no = (worker_no + 1) % worker_count
    # multiprocessing
    processes = []
    for worker in workers:
        processes.append(multiprocessing.Process(target=worker.job_run,
                                                 name=worker.worker_name,
                                                 args=()))
    for p in processes:
        p.daemon = True
        p.start()
        logging.info("p.pid:{} p.name:{} p.is_alive {}".format(p.pid, p.name, p.is_alive()))

    processes = [p for p in processes if p.is_alive()]
    for p in processes:
        p.join()
    logging.info("get document Complete.")


class TopicWorker(Worker):
    TOPIC_URL = "http://vip.stock.finance.sina.com.cn/q/go.php/vReport_List/kind/search/index.phtml?symbol={" \
                "}&t1=all&p={}"
    URL_EXPR = "vip.stock.finance.sina.com.cn/q/go.php/vReport_Show/kind/search/rptid"

    def job_consumer(self):
        while self.job_pool:
            logging.info("{0} left job {1}".format(self.worker_name, len(self.job_pool)))
            job = self.job_pool.pop()
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %X")
            code, page = job["code"], job["page"]
            time.sleep(1)
            logging.info("{0} consumer use stock {1} page {2}".format(self.worker_name, code, page))
            r = requests.get(self.TOPIC_URL.format(code, page))
            r.encoding = 'gb2312'
            soup = BeautifulSoup(r.text, "html.parser")
            # topic
            links = soup.find_all("a", href=re.compile(self.URL_EXPR))
            # if topic list is none, pass
            if not links:
                continue
            # if last link exist in mongodb, pass page
            last_link = links[-1]
            pass_page = self.db.stock_report.find({"url": last_link.get("href")}).count()
            for link in links:
                d = {"url": link.get("href"),
                     "title": link.get("title"),
                     "code": code,
                     "timestamp": timestamp}
                self.db.stock_report.update({"url": d["url"]}, {"$set": d}, True)
            # if page>1, pass
            if page > 1:
                continue
            if pass_page > 0:
                continue
            page_buttons = soup.find_all("a", onclick=re.compile("set_page_num"))
            # if button is not match re, pass
            if not page_buttons:
                continue
            last_onclick = page_buttons[-1].get("onclick")
            re_page_num = re.compile("(?<=set_page_num\(')(.*)?(?='\))")
            re_result = re_page_num.search(last_onclick)
            # if button is not match re, pass
            if re_result is None:
                continue
            max_page = int(re_result.group())
            logging.info("{0} stock {1} max page {2}".format(self.worker_name, code, max_page))
            other_pages = range(2, max_page + 1)
            self.job_pool.extend([{"code": code, "page": p} for p in other_pages])
        logging.info("{0} consumer complete.".format(self.worker_name))


class DocumentWorker(Worker):
    def job_consumer(self):
        while self.job_pool:
            logging.info("{0} left job {1}".format(self.worker_name, len(self.job_pool)))
            url = self.job_pool.pop()
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %X")
            logging.info("{} consumer use url {}".format(self.worker_name, url))
            r = requests.get(url)
            r.encoding = 'gb2312'
            try:
                soup = BeautifulSoup(r.text, "html.parser")
                d = {"timestamp": timestamp}
                content_select = soup.find("div", class_="content")
                # no content
                if content_select is None:
                    pass
                else:
                    # set title
                    title_select = content_select.find("h1")
                    if title_select is None:
                        pass
                    else:
                        d.setdefault("title", title_select.text)
                    # set creab span
                    creab_select = content_select.find("div", class_="creab")
                    if creab_select is None:
                        pass
                    else:
                        span = [e.text for e in creab_select.findAll("span")]
                        # noinspection PyTypeChecker
                        d.setdefault("span", span)
                    # document, if none then set --
                    document_select = content_select.find("div", class_="blk_container")
                    if document_select is None:
                        content = "--"
                    else:
                        content = document_select.text.strip().replace("\xa0\xa0\xa0\n", "")
                    d.setdefault("content", content)
                self.db.stock_report.update({"url": url}, {"$set": d}, False)
            except Exception as e:
                d = {"content": "ERR", "timestamp": timestamp}
                self.db.stock_report.update({"url": url}, {"$set": d}, False)
                logging.warning(e)
        logging.info("{0} consumer complete.".format(self.worker_name))
