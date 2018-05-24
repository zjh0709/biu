from job import db, index_mapper, zk_check
from job.ProgressBar import ProgressBar
import tushare as ts
import datetime
import logging


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
            db.stock_basics.update({'code': code}, {'$set': {'recover_date': recover_date}}, False)
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


def check_is_open():
    w = datetime.datetime.now().strftime("%w")
    logging.info("week {}".format(w))
    if w not in ["1", "2", "3", "4", "5"]:
        return False
    t = datetime.datetime.now().strftime("%X")
    logging.info("time {}".format(t))
    if "09:30" <= t <= "11:35":
        return True
    if "13:00" <= t <= "15:05":
        return True
    return False


@zk_check()
def live_index_data() -> None:
    if not check_is_open():
        logging.warning("market is closed.")
        return None
    dt = datetime.datetime.now().strftime("%Y-%m-%d")
    df = ts.get_index()
    df["code"] = df.apply(lambda x: index_mapper.get(x.code, "None"), axis=1)
    df.rename(columns={"change": "p_change"}, inplace=True)
    df["price_change"] = df.apply(lambda x: x["close"] - x["preclose"], axis=1)
    df["volume"] = df.apply(lambda x: x["volume"] * 0.01, axis=1)
    df["date"] = dt
    data = df[["date", "code", "open", "close", "high", "low", "volume", "p_change", "price_change", "amount",
               "preclose"]].to_dict(orient="records")
    bar = ProgressBar(total=len(data))
    for d in data:
        bar.move()
        db.index_data.update({"code": d["code"], "date": d["date"]}, {"$set": d}, True)
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
    live_stock_data()
