from job.util.ProgressBar import ProgressBar
from job.util.Zk import zk_check
from job.util.Mongo import db
from job.util.Market import market_check
import tushare as ts
import datetime
import logging


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


@zk_check()
def recover_index_data() -> None:
    bar = ProgressBar(total=len(index_mapper))
    for code in index_mapper.values():
        bar.move()
        df = ts.get_hist_data(code)
        if df is None:
            bar.log("code {} count None".format(code))
            continue
        df.reset_index(inplace=True)
        data = df.to_dict(orient="records")
        db.index_data.remove({"code": code})
        db.index_data.insert(data)
        bar.log("code {} count {}".format(code, len(data)))


@zk_check()
def recover_stock_data() -> None:
    recover_date = datetime.datetime.now().strftime("%Y-%m-%d")
    stocks = [d["code"] for d in
              db.stock_basics.find({
                  "$or": [
                      {"recover_date": {"$exists": False}},
                      {"recover_date": {"$lt": recover_date}}
                  ]
              },
                  {"code": 1, "_id": 0})]
    bar = ProgressBar(total=len(stocks))
    for code in stocks:
        bar.move()
        df = ts.get_hist_data(code)
        if df is None:
            db.stock_basics.update({'code': code},
                                   {'$set': {'recover_date': recover_date}},
                                   False)
            bar.log("code {} is None".format(code))
            continue
        df.reset_index(inplace=True)
        df["code"] = code
        data = df.to_dict(orient="records")
        db.stock_data.remove({"code": code})
        db.stock_data.insert(data)
        db.stock_basics.update({'code': code}, {'$set': {'recover_date': recover_date}}, False)
        bar.log("code {} count {}".format(code, len(data)))


@zk_check()
def update_stock_data_by_date(dt: str) -> None:
    stocks = [d["code"] for d in db.stock_basics.find({}, {"code": 1, "_id": 0})]
    bar = ProgressBar(total=len(stocks))
    for code in stocks:
        bar.move()
        df = ts.get_hist_data(code, start=dt, end=dt)
        if df is None:
            bar.log("code: {} is None".format(code))
        else:
            df.reset_index(inplace=True)
            df["code"] = code
            data = df.to_dict(orient="records")
            for d in data:
                db.stock_data.update({"code": d["code"], "date": d["date"]}, {"$set": d}, True)
                bar.log("code: %(code)s, date %(date)s" % d)


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
               "p_change", "price_change", "amount", "preclose"]]\
        .to_dict(orient="records")
    bar = ProgressBar(total=len(data))
    for d in data:
        bar.move()
        db.index_data.update({"code": d["code"], "date": d["date"]},
                             {"$set": d},
                             True)
        bar.log("code %(code)s update success." % d)


@zk_check()
def live_stock_data() -> None:
    if not check_is_open():
        logging.warning("market is closed.")
        return None
    dt = datetime.datetime.now().strftime("%Y-%m-%d")
    df = ts.get_today_all()
    df.rename(columns=dict(trade="close", turnoverratio="turnover", changepercent="p_change", settlement="preclose"),
              inplace=True)
    df['price_change'] = df.apply(lambda x: x['close'] - x['preclose'], axis=1)
    df['volume'] = df.apply(lambda x: x["volume"] * 0.01, axis=1)
    df['date'] = dt
    data = df[
        ["date", "code", "open", "close", "high", "low", "volume", "p_change", "price_change", "turnover", "amount",
         "preclose", "per", "pb", "mktcap", "nmc"]].to_dict(orient="records")
    bar = ProgressBar(total=len(data))
    for d in data:
        bar.move()
        db.stock_data.update({"code": d["code"], "date": d["date"]}, {"$set": d}, True)
        bar.log("code %(code)s update success." % d)


if __name__ == "__main__":
    live_index_data()
