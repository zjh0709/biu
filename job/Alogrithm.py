from job import db, zk_check
import datetime
from itertools import chain, groupby
from collections import Counter
from operator import itemgetter
import logging


@zk_check()
def get_hot_keyword(dt: str = datetime.datetime.now().strftime("%Y-%m-%d"), ft: float = 6.0):
    data = list(db.stock_data.find({"date": dt, "p_change": {"$gte": ft}},
                                   {"_id": 0, "code": 1, "p_change": 1}))
    logging.info("data count {}".format(len(data)))
    code = list(map(lambda x: x["code"], data))
    code_keyword = list(db.stock_report.find({"code": {"$in": code},
                                              "keyword": {"$exists": True}},
                                             {"_id": 0, "code": 1, "keyword": 1, "span": 1}))
    anti_words = list(map(lambda x: x["word"], db.anti_word.find({}, {"_id": 0, "word": 1})))

    for d in code_keyword:
        publish_date = [dd.replace("日期：", "") for dd in d["span"] if "日期：" in dd]
        d["publish_date"] = publish_date[0] if len(publish_date) > 0 else ""
        d["keyword"] = list(set(d["keyword"]).difference(anti_words))
        del d["span"]
    keep_day = 90
    keep_start = (datetime.datetime.strptime(dt, "%Y-%m-%d") + datetime.timedelta(days=-keep_day)) \
        .strftime("%Y-%m-%d")
    code_keyword = list(filter(lambda x: x["publish_date"] >= keep_start, code_keyword))
    code_keyword.sort(key=lambda x: x["code"])
    code_keyword_mapper = {}
    for k, v in groupby(code_keyword, key=lambda x: x["code"]):
        word = chain.from_iterable([d["keyword"] for d in v])
        code_keyword_mapper.setdefault(k, word)
    hot_keyword = Counter(chain.from_iterable((code_keyword_mapper.values()))).most_common(50)
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
    basics = db.stock_basics.find({"code": {"$in": list(code_keyword_mapper.keys())}},
                                  {"_id": 0, "code": 1, "pb": 1, "pe": 1, "name": 1,
                                   "outstanding": 1, "industry": 1, "concept": 1})
    stock_basics_mapper = {d["code"]: d for d in basics}

    p_change = list(db.stock_data.find({"date": dt, "code": {"$in": list(code_keyword_mapper.keys())}},
                                       {"_id": 0, "code": 1, "p_change": 1, "close": 1}))
    p_change.sort(key=lambda x: x["p_change"], reverse=True)
    for d in p_change:
        current_code = d["code"]
        current_p_change = float(d["p_change"])
        current_close = float(d["close"])
        d["keyword"] = ",".join(set(code_keyword_mapper.get(current_code, []))
                                .intersection(hot_keyword))
        d["name"] = stock_basics_mapper.get(current_code, {}).get("name", "UNKNOWN")
        d["pb"] = float(stock_basics_mapper.get(current_code, {}).get("pb", 0))*(1+current_p_change/100)
        d["pe"] = float(stock_basics_mapper.get(current_code, {}).get("pe", 0))*(1+current_p_change/100)
        d["value"] = float(stock_basics_mapper.get(current_code, {}).get("outstanding", 0))*current_close
        d["concept"] = ",".join(stock_basics_mapper.get(current_code, {}).get("concept", []))
        print("%(code)s %(name)s %(p_change)f %(pb)f %(pe)f %(value)f %(concept)s %(keyword)s" % d)


if __name__ == '__main__':
    get_hot_keyword()
