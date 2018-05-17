from job import db
from job.ProgressBar import ProgressBar
from itertools import groupby
from operator import itemgetter


def update_feature() -> None:
    stocks = list(
        db.stock_basics.find({}, {"_id": 0, "code": 1, "industry": 1, "concept": 1, "area": 1})
    )
    industry, concept, area = [], [], []
    for stock in stocks:
        code = stock["code"]
        industry.extend([(("industry", d), code) for d in stock.get("industry", [])])
        concept.extend([(("concept", d), code) for d in stock.get("concept", [])])
        area.extend([(("area", d), code) for d in stock.get("area", [])])
    industry.sort(key=itemgetter(0))
    concept.sort(key=itemgetter(0))
    area.sort(key=itemgetter(0))
    industry_group = [(k, list(map(itemgetter(1), v))) for
                      k, v in groupby(industry, key=itemgetter(0))]
    concept_group = [(k, list(map(itemgetter(1), v))) for
                     k, v in groupby(concept, key=itemgetter(0))]
    area_group = [(k, list(map(itemgetter(1), v))) for
                  k, v in groupby(area, key=itemgetter(0))]
    feature_update = []
    feature_update.extend(industry_group)
    feature_update.extend(concept_group)
    feature_update.extend(area_group)
    feature_update = dict(feature_update)
    # find current feature
    current_feature = [((d["class"], d["name"]), d["stock"]) for
                       d in db.feature_stock.find({}, {"_id": 0, "class": 1, "name": 1, "stock": 1})]
    current_feature = dict(current_feature)
    bar = ProgressBar(total=len(feature_update))
    for k, v in feature_update.items():
        bar.move()
        current_v = current_feature.get(k, [])
        new_v = list(set(v + current_v))
        if len(new_v) == len(current_v):
            bar.log("class {0} name {1} not update.".format(*k))
            continue
        db.feature_stock.update({"class": k[0], "name": k[1]}, {"$set": {"stock": new_v}}, True)
        bar.log("class {0} name {1} update {2}.".format(k[0], k[1],
                                                        set(new_v).difference(current_v)))


def feature_cube() -> None:
    pass


if __name__ == "__main__":
    update_feature()
