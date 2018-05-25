# coding: utf-8
from concurrent.futures import ThreadPoolExecutor as Pool
from concurrent.futures import as_completed
import time

URLS = ['http://qq.com', 'http://sina.com', 'http://www.baidu.com']


def task(url):
    time.sleep(5)
    return url


if __name__ == '__main__':
    a = time.time()
    print(list(map(task, URLS)))
    b = time.time()
    print(b - a)

    a = time.time()
    with Pool(max_workers=3) as executor:
        result = executor.map(task, URLS)
        for ret in result:
            print(ret)
    b = time.time()
    print(b - a)
