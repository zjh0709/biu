from job import db, zk_check
import datetime
from itertools import chain, groupby
from collections import Counter
from operator import itemgetter
import logging


@zk_check()
def get_hot_keyword(dt: str = datetime.datetime.now().strftime("%Y-%m-%d"), ft: float = 6.0):
    data = [d for d in db.stock_data.find({"date": dt, "p_change": {"$gte": ft}},
                                          {"_id": 0, "code": 1, "p_change": 1})]
    logging.info("data count {}".format(len(data)))
    code = [d["code"] for d in data]
    code_keyword = [d for d in
                    db.stock_report.find(dict(code={"$in": code}, keyword={"$exists": True}),
                                         {"_id": 0, "code": 1, "keyword": 1})]
    code_keyword.sort(key=lambda x: x["code"])
    code_keyword_mapper = {}
    for k, v in groupby(code_keyword, key=lambda x: x["code"]):
        word = set(chain.from_iterable([d["keyword"] for d in v]))
        code_keyword_mapper.setdefault(k, word)
    hot_keyword = Counter(chain.from_iterable((code_keyword_mapper.values()))).most_common(50)
    min_hot_count = min(map(itemgetter(1), hot_keyword))
    hot_keyword = list(filter(lambda x: x[1] > min_hot_count, hot_keyword))
    logging.info(hot_keyword)


if __name__ == '__main__':
    get_hot_keyword()
