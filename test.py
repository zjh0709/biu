# coding: utf-8
from concurrent.futures import ThreadPoolExecutor as Pool
from concurrent.futures import as_completed
import time

URLS = ['http://qq.com', 'http://sina.com', 'http://www.baidu.com']

fuck = []

def task(url):
    global fuck
    fuck.append(url)
    return url


if __name__ == '__main__':
    with Pool(max_workers=3) as executor:
        result = executor.map(task, URLS)
        for ret in result:
            print(ret)
    print(fuck)
