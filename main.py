import argparse
import datetime
from job import DataInfo, BasicInfo, FeatureInfo


if __name__ == "__main__":
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    ap = argparse.ArgumentParser()
    ap.add_argument("-a", "--action", help="", required=True)
    ap.add_argument("-d", "--date", help=today, required=False, default=today)
    args = ap.parse_args()
    if args.action == "basic":
        BasicInfo.update_stock_basic()
    elif args.action == "recover":
        DataInfo.recover_index_data()
        DataInfo.recover_stock_data()
    elif args.action == "live":
        DataInfo.live_index_data()
        DataInfo.live_stock_data()
    elif args.action == "date":
        DataInfo.update_stock_data_by_date(args.date)
    elif args.action == "feature":
        FeatureInfo.update_feature()
