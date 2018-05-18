from job import client, db, zk_check
from job.ProgressBar import ProgressBar
from job.Worker import Worker
import tushare as ts
import time
import multiprocessing
import logging


@zk_check()
def get_news_url(num: int = 1000) -> None:
    df = ts.get_latest_news(top=num, show_content=False)
    df['timestamp'] = int(time.time())
    data = df.to_dict(orient="records")
    bar = ProgressBar(total=len(data))
    for d in data:
        bar.move()
        db.break_news.update({"url": d["url"]}, {"$set": d}, True)
        try:
            bar.log("title {}".format(d.get("title")))
        except UnicodeEncodeError as e:
            bar.log(e)


@zk_check()
def get_news_content() -> None:
    url = [d["url"] for d in db.break_news.find({"content": {"$exists": False}})]
    workers = [ContentWorker(address=client.address, db_name=db.name, worker_name=worker_name) for
               worker_name in ["content_worker1", "content_worker2", "content_worker3"]]

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


class ContentWorker(Worker):
    def job_consumer(self) -> None:
        while self.job_pool:
            job = self.job_pool.pop()
            content = ts.latest_content(job)
            content = content if content is not None else "--"
            self.db.break_news.update({"url": job}, {"$set": {"content": content}}, False)
            logging.info("{} success. left {}".format(self.worker_name, len(self.job_pool)))


if __name__ == "__main__":
    get_news_content()
