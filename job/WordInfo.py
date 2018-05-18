from job import db
from job.BaiduNlp import BaiduNlp
from job.ProgressBar import ProgressBar


def get_report_word() -> None:
    docs = [d for d in db.stock_report.find({"word": {"$exists": False},
                                             "content": {"$exists": True}},
                                            {"_id": 0, "url": 1, "title": 1, "content": 1})]
    baidu_nlp = BaiduNlp()
    bar = ProgressBar(total=len(docs))
    for d in docs:
        bar.move()
        word = list(baidu_nlp.word(d.get("title", "") + d["content"]))
        db.stock_report.update({"url": d["url"]}, {"$set": {"word": word}})
        bar.log("url {} success.".format(d["url"]))


if __name__ == '__main__':
    get_report_word()

