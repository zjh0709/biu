import pymongo
import logging


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

    def job_consumer(self):
        logging.info("{0} consumer complete.".format(self.worker_name))

    def job_run(self):
        self.job_consumer()
        self.quit()

    def quit(self):
        self.client.close()
        logging.info("{} complete".format(self.worker_name))
