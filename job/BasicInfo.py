from job import db, ProgressBar
import tushare as ts


def update_stock_basic():
    df = ts.get_stock_basics()
    df.reset_index(inplace=True)
    data = df.to_dict(orient="records")
    # update industry
    industry_df = ts.get_industry_classified()
    industry_df = industry_df.groupby(["code"])["c_name"].apply(lambda x: x.tolist()).reset_index()
    industry_mapper = dict([(d["code"], d["c_name"]) for
                            d in industry_df.to_dict(orient="records")])
    # update concept
    concept_df = ts.get_concept_classified()
    concept_df = concept_df.groupby(["code"])["c_name"].apply(lambda x: x.tolist()).reset_index()
    concept_mapper = dict([(d["code"], d["c_name"]) for
                           d in concept_df.to_dict(orient="records")])
    # update area
    area_df = ts.get_area_classified()
    area_df = area_df.groupby(["code"])["area"].apply(lambda x: x.tolist()).reset_index()
    area_mapper = dict([(d["code"], d["area"]) for
                        d in area_df.to_dict(orient="records")])
    bar = ProgressBar(total=len(data))
    for d in data:
        d["industry"] = list(set(industry_mapper.get(d["code"], []) + [d["industry"]]))
        d["concept"] = concept_mapper.get(d["code"], [])
        d["area"] = list(set(area_mapper.get(d["code"], []) + [d["area"]]))
        bar.move()
        db.stock_basics.update({"code": d["code"]}, {"$set": d}, True)
        bar.log("code: {} name: {} industry {} concept {} area {}"
                .format(d["code"], d["name"], d["industry"], d["concept"], d["area"]))


if __name__ == "__main__":
    print(update_stock_basic())
