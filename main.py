import argparse
import datetime


if __name__ == "__main__":
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    ap = argparse.ArgumentParser()
    ap.add_argument("-a", "--action", help="", required=True)
    ap.add_argument("-t", "--type", help="report", required=False)
    ap.add_argument("-d", "--date", help=today, required=False, default=today)
    ap.add_argument("-n", "--num", help="", required=False)
    args = ap.parse_args()
    if args.action == "basic":
        from job import BasicInfo
        BasicInfo.update_stock_basic()
    elif args.action == "recover":
        from job import DataInfo
        DataInfo.recover_index_data()
        DataInfo.recover_stock_data()
    elif args.action == "live":
        from job import DataInfo
        DataInfo.live_index_data()
        DataInfo.live_stock_data()
    elif args.action == "date":
        from job import DataInfo
        DataInfo.update_index_data_by_date(args.date)
        DataInfo.update_stock_data_by_date(args.date)
    elif args.action == "feature":
        from job import FeatureInfo
        FeatureInfo.update_feature()
    elif args.action == "news":
        from job import NewsInfo
        NewsInfo.get_news_url(int(args.num) if args.num is not None else 200)
        NewsInfo.get_news_content()
    elif args.action == "report":
        from job import ReportInfo
        ReportInfo.get_topic()
        ReportInfo.get_document()
    elif args.type == "report" and args.action == "word":
        from job import WordInfo
        WordInfo.get_report_word(int(args.num) if args.num is not None else 1000)
    elif args.type == "report" and args.action == "keyword":
        from job import WordInfo
        WordInfo.get_report_keyword(int(args.num) if args.num is not None else 1000)
    elif args.type == "news" and args.action == "word":
        from job import WordInfo
        WordInfo.get_news_word(int(args.num) if args.num is not None else 1000)
    elif args.type == "news" and args.action == "keyword":
        from job import WordInfo
        WordInfo.get_news_keyword(int(args.num) if args.num is not None else 1000)
    elif args.action == "hot":
        from job import Predict
        Predict.get_hot_keyword(dt=args.date, ft=float(args.num) if args.num is not None else 7.0)
    elif args.action == "server" and args.type == "article":
        from server import ArticleServer
        ArticleServer.article_server_run("0.0.0.0", 62345)
