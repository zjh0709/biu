from job import client, db
from job.ProgressBar import ProgressBar
import pymongo
import tushare as ts
import time
import multiprocessing
import logging


def get_news_url(num: int = 1000) -> None:
    df = ts.get_latest_news(top=num, show_content=False)
    df['timestamp'] = int(time.time())
    data = df.to_dict(orient="records")
    bar = ProgressBar(total=len(data))
    for d in data:
        bar.move()
        db.break_news.update({"url": d["url"]}, {"$set": d}, True)
        bar.log("title {}".format(d.get("title")))


def get_news_content() -> None:
    url = [d["url"] for d in db.break_news.find({"content": {"$exists": False}})]
    workers = [Worker(address=client.address, db_name=db.name, worker_name=worker_name) for
               worker_name in ["worker1", "worker2", "worker3"]]

    # distribute stock
    worker_no = 0
    worker_count = len(workers)
    for arg in url:
        workers[worker_no].job_append(arg)
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
    logging.info("Mission Complete.")


class Worker(object):
    def __init__(self, address: tuple, db_name: str, worker_name):
        self.worker_name = worker_name
        self.client = pymongo.MongoClient(*address)
        self.db = self.client.get_database(db_name)
        self.job_pool = []

    def job_append(self, job):
        self.job_pool.append(job)

    def job_extend(self, jobs):
        self.job_pool.extend(jobs)

    def job_run(self):
        while self.job_pool:
            job = self.job_pool.pop()
            content = ts.latest_content(job)
            content = content if content is not None else "--"
            self.db.break_news.update({"url": job}, {"$set": {"content": content}}, False)
            logging.info("{} {} success. left {}".format(self.worker_name, job, len(self.job_pool)))
        self.quit()

    def quit(self):
        self.client.close()
        logging.info("{} complete.".format(self.worker_name))


if __name__ == "__main__":
    get_news_content()
