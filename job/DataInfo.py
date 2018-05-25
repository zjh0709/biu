from job.util.Zk import zk_check
from job.util.Mongo import db
from job.util.Market import market_check
import tushare as ts
import datetime
import logging
from concurrent.futures import ThreadPoolExecutor
from itertools import chain
from functools import partial

index_mapper = {
    "000001": "sh000001",
    "000002": "sh000002",
    "000003": "sh000003",
    "000008": "sh000008",
    "000009": "sh000009",
    "000010": "sh000010",
    "000011": "sh000011",
    "000012": "sh000012",
    "000016": "sh000016",
    "000017": "sh000017",
    "000300": "sh000300",
    "000905": "sh000905",
    "399001": "sz399001",
    "399002": "sz399002",
    "399003": "sz399003",
    "399004": "sz399004",
    "399005": "sz399005",
    "399006": "sz399006",
    "399008": "sz399008",
    "399100": "sz399100",
    "399101": "sz399101",
    "399106": "sz399106",
    "399107": "sz399107",
    "399108": "sz399108",
    "399333": "sz399333",
    "399606": "sz399606"
}


def get_hist_data(code: str, start: str = None, end: str = None):
    df = ts.get_hist_data(code, start, end)
    if df is not None:
        df.reset_index(inplace=True)
        df["code"] = code
        data = df.to_dict(orient="records")
        logging.info("code {} complete".format(code))
    else:
        logging.warning("code {} is None".format(code))
        data = []
    return data


def insert(collection_name, index_data):
    db.get_collection(collection_name).insert(index_data)
    logging.info("insert code %(code)s success." % index_data[0])


def update(collection_name, d):
    db.get_collection(collection_name) \
        .update({"code": d["code"], "date": d["date"]}, {"$set": d}, True)
    logging.info("update code %(code)s success." % d)


@zk_check()
def recover_index_data() -> None:
    db.index_data.drop()
    logging.info("drop complete")
    insert_function = partial(insert, "index_data")
    with ThreadPoolExecutor(max_workers=3) as executor:
        result = executor.map(get_hist_data, list(index_mapper.values()))
        result = filter(lambda x: x != [], result)
        executor.map(insert_function, result)


@zk_check()
def recover_stock_data() -> None:
    stocks = [d["code"] for d in db.stock_basics.find({}, {"code": 1, "_id": 0})]
    db.stock_data.drop()
    logging.info("drop complete")
    insert_function = partial(insert, "stock_data")
    with ThreadPoolExecutor(max_workers=3) as executor:
        result = executor.map(get_hist_data, stocks)
        result = filter(lambda x: x != [], result)
        executor.map(insert_function, result)


@zk_check()
def update_index_data_by_date(dt: str) -> None:
    stocks = index_mapper.values()
    update_function = partial(update, "index_data")
    with ThreadPoolExecutor(max_workers=3) as executor:
        result = executor.map(get_hist_data, stocks, [dt] * len(stocks), [dt] * len(stocks))
        executor.map(update_function, chain.from_iterable(result))


@zk_check()
def update_stock_data_by_date(dt: str) -> None:
    stocks = [d["code"] for d in db.stock_basics.find({}, {"code": 1, "_id": 0})]
    update_function = partial(update, "stock_data")
    with ThreadPoolExecutor(max_workers=3) as executor:
        result = executor.map(get_hist_data, stocks, [dt] * len(stocks), [dt] * len(stocks))
        executor.map(update_function, chain.from_iterable(result))


@zk_check()
@market_check()
def live_index_data() -> None:
    dt = datetime.datetime.now().strftime("%Y-%m-%d")
    df = ts.get_index()
    df["code"] = df.apply(lambda x: index_mapper.get(x.code, "None"), axis=1)
    df.rename(columns={"change": "p_change"}, inplace=True)
    df["price_change"] = df.apply(lambda x: x["close"] - x["preclose"], axis=1)
    df["volume"] = df.apply(lambda x: x["volume"] * 0.01, axis=1)
    df["date"] = dt
    data = df[["date", "code", "open", "close", "high", "low", "volume",
               "p_change", "price_change", "amount", "preclose"]] \
        .to_dict(orient="records")

    update_function = partial(update, "index_data")
    with ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(update_function, data)


@zk_check()
def live_stock_data() -> None:
    dt = datetime.datetime.now().strftime("%Y-%m-%d")
    df = ts.get_today_all()
    df.rename(columns=dict(trade="close", turnoverratio="turnover", changepercent="p_change", settlement="preclose"),
              inplace=True)
    df['price_change'] = df.apply(lambda x: x['close'] - x['preclose'], axis=1)
    df['volume'] = df.apply(lambda x: x["volume"] * 0.01, axis=1)
    df['date'] = dt
    data = df[
        ["date", "code", "open", "close", "high", "low", "volume", "p_change", "price_change",
         "turnover", "amount", "preclose", "per", "pb", "mktcap", "nmc"]]\
        .to_dict(orient="records")
    update_function = partial(update, "stock_data")
    with ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(update_function, data)


if __name__ == "__main__":
    live_stock_data()