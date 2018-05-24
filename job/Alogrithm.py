from job import db, zk_check
import datetime
from itertools import chain, groupby
from collections import Counter
from operator import itemgetter
import logging


@zk_check()
def get_hot_keyword(dt: str = datetime.datetime.now().strftime("%Y-%m-%d"), ft: float = 8.0):
    data = list(db.stock_data.find({"date": dt, "p_change": {"$gte": ft}},
                                   {"_id": 0, "code": 1, "p_change": 1}))
    logging.info("data count {}".format(len(data)))
    code = [d["code"] for d in data]
    code_keyword = list(db.stock_report.find({"code": {"$in": code},
                                              "keyword": {"$exists": True}},
                                             {"_id": 0, "code": 1, "keyword": 1}))
    code_keyword.sort(key=lambda x: x["code"])
    code_keyword_mapper = {}
    for k, v in groupby(code_keyword, key=lambda x: x["code"]):
        word = set(chain.from_iterable([d["keyword"] for d in v]))
        code_keyword_mapper.setdefault(k, word)
    hot_keyword = Counter(chain.from_iterable((code_keyword_mapper.values()))).most_common(100)
    min_hot_count = min(map(itemgetter(1), hot_keyword))
    hot_keyword = list(map(itemgetter(0),
                           filter(lambda x: x[1] > min_hot_count, hot_keyword)))
    logging.info("********* key word *********")
    logging.info(hot_keyword)
    logging.info("********* stock *********")
    condition = {"$or": [{"keyword": k} for k in hot_keyword]}
    report = list(db.stock_report.find(condition, {"_id": 0, "code": 1, "keyword": 1}))
    code_keyword_mapper = {}
    for d in report:
        tmp = code_keyword_mapper.get(d["code"], [])
        tmp.extend(d["keyword"])
        code_keyword_mapper[d["code"]] = tmp
    p_change = list(db.stock_data.find({"date": dt, "code": {"$in": list(code_keyword_mapper.keys())}},
                                       {"_id": 0, "code": 1, "p_change": 1}))
    p_change.sort(key=lambda x: x["p_change"], reverse=True)
    for d in p_change:
        d["keyword"] = ",".join(set(code_keyword_mapper.get(d["code"], []))
                                .intersection(hot_keyword))
        print("%(code)s %(p_change)f %(keyword)s" % d)


if __name__ == '__main__':
    get_hot_keyword()
