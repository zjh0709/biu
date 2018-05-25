import datetime
import logging


def market_check():
    def wrape(func):
        def todo(*args, **kwargs):
            flag = False
            w = datetime.datetime.now().strftime("%w")
            t = datetime.datetime.now().strftime("%X")
            logging.info("week {} time {}".format(w, t))
            if w in ["1", "2", "3", "4", "5"] and ("09:30" <= t <= "11:35" or "13:00" <= t <= "15:05"):
                flag = True
            if flag is True:
                rs = func(*args, **kwargs)
                return rs
            else:
                logging.info("market is closed.")
                return None
        return todo
    return wrape


