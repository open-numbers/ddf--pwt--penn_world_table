# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import os

from ddf_utils.str import to_concept_id

# configuration of file path
source = "../source/pwt1001.xlsx"
out_dir = "../../"


def extract_concepts(concepts):
    cdf = concepts.set_index("Variable name").dropna()
    cdf.index.name = "concept"
    cdf.columns = ["name"]

    cdf = cdf.rename(index={"country": "name", "countrycode": "country"})
    cdf.loc["name", "name"] = "Name"

    cdf["concept_type"] = "measure"

    cdf.loc["country", "concept_type"] = "entity_domain"
    cdf.loc["name", "concept_type"] = "string"
    cdf.loc["year", "concept_type"] = "time"
    cdf.loc["currency_unit", "concept_type"] = "string"

    return cdf.reset_index()


def extract_entities_country(data):
    country = data[["countrycode", "country", "currency_unit"]].copy()
    country = country.drop_duplicates()
    country.columns = ["country", "name", "currency_unit"]
    country["country"] = country["country"].map(to_concept_id)

    return country


def extract_datapoints(data):
    dps = data.drop(["country", "currency_unit"], axis=1).copy()
    dps = dps.rename(columns={"countrycode": "country"})
    dps["country"] = dps["country"].map(to_concept_id)

    dps = dps.set_index(["country", "year"])

    for k, df in dps.items():
        df_ = df.reset_index().dropna()

        yield k, df_


if __name__ == "__main__":
    print("reading source files...")
    concepts = pd.read_excel(source, sheet_name="Legend")
    data = pd.read_excel(source, sheet_name="Data")

    print("creating concept files...")
    cdf = extract_concepts(concepts)
    path = os.path.join(out_dir, "ddf--concepts.csv")
    cdf.to_csv(path, index=False)

    print("creating entities files...")
    country = extract_entities_country(data)
    path = os.path.join(out_dir, "ddf--entities--country.csv")
    country.to_csv(path, index=False)

    print("creating datapoints files...")
    for k, df in extract_datapoints(data):
        if str(k).startswith("i_"):
            print(f"{k}: column starts with i_ are text columns, skipping.")
        else:
            path = os.path.join(out_dir, "ddf--datapoints--{}--by--country--year.csv".format(k))
            df.to_csv(path, index=False)

    # print('creating index file...')
    # create_index_file(out_dir)

    print("Done.")
