"""Improve GRID matches"""

import pandas as pd
import spacy
from spacy.tokens.doc import Doc
from pandas import DataFrame
from rapidfuzz.fuzz import partial_ratio
from rapidfuzz.process import extractOne


def find_wrong_matches(true_grid: str, matched_grid: str) -> str | None:
    """"""
    if true_grid == "nan" and matched_grid == "nan":
        return None
    if true_grid == "nan" and matched_grid != "nan":
        return matched_grid
    if true_grid != "nan":
        return true_grid


def find_organisation(row: Doc) -> list[str]:
    """Finds an organisation in the affiliations"""
    return [i.text for i in row.ents if i.label_ == "ORG"]


def get_organisation_from_csv(institutes: DataFrame, organisations: list[str], country: str, cache: dict) -> str | None:
    """Finds an existing institutional organisation from affiliation data"""
    values = []
    for org in organisations:
        if org in cache:
            values.append(cache[org])
        if country is not None:
            institute_names = institutes[institutes["country"] == country]
        if country is None or institute_names.isnull:
            institute_names = institutes["name"]
        matched_organisation_data = extractOne(org, institute_names, scorer=partial_ratio, score_cutoff=80)
        if matched_organisation_data is not None:
            matched_organisation, score, _ = matched_organisation_data
            returned_value = institutes[institutes["name"] == matched_organisation].iloc[0]["grid_id"]
            cache[org] = [returned_value, score]
            values.append(returned_value)
    if len(values) > 0:
        corrected_values = []
        for val in values:
            try:
                value = [val[0],int(val[1])]
            except ValueError:
                value = [val[0],0]
            corrected_values.append(value)
        sorted_list = sorted(corrected_values, key=lambda x: x[1])
        valuable_matches = [l for l in sorted_list if l[1] > 60]
        return valuable_matches[::-1][0][0] if len(valuable_matches) > 0 else None


def correct_grid(data: DataFrame) -> DataFrame:
    """Replace rapidfuzz matches or Nones with manually matched values if exist"""
    data["AffiliationGRID"] = data["AffiliationGRID"].astype(str)
    data["AffiliationMatchedGRID"] = data["AffiliationMatchedGRID"].astype(str)
    data["AffiliationMatchedGRID"] = data.apply(lambda row: find_wrong_matches(row["AffiliationGRID"],
                                                                row["AffiliationMatchedGRID"]), axis=1)
    return data

def find_unfound_grid(data: DataFrame) -> DataFrame:
    """Returns dataframe after searching for GRID ids more thoroughly"""
    data_without_matched_grid = data[data["AffiliationMatchedGRID"].isna()]
    has_affiliation = data_without_matched_grid[data_without_matched_grid["Affiliations"].notna()]
    nlp = spacy.load("en_core_web_sm", disable=["tagger", "parser", "textcat", "senter", "lemmatizer"])
    has_affiliation["Organisation"] = has_affiliation["Affiliations"].apply(nlp)
    has_affiliation["Organisation"] = has_affiliation["Organisation"].apply(find_organisation)
    institutes_df = pd.read_csv("institutes_data.csv")[["grid_id","name"]].set_index("grid_id")
    adresses_df = pd.read_csv("addresses_data.csv", low_memory=False)[["grid_id", "country"]].set_index("grid_id")
    institutes_combined_df = institutes_df.join(adresses_df)
    institutes_combined_df["country"].replace({"United Kingdom": "UK", "United States": "USA"}, inplace=True)
    institutes_combined_df = institutes_combined_df.reset_index()
    cache_dict = dict()
    has_affiliation["AffiliationMatchedGRID"] = has_affiliation.apply(
            lambda row: get_organisation_from_csv(institutes_combined_df, row["Organisation"],
                row["Country"], cache_dict) if row["Organisation"] is not None else None, axis=1)
    data.update(has_affiliation["AffiliationMatchedGRID"])
    return data


if __name__ == "__main__":
    data = pd.read_csv("transformed_data.csv")
    data = correct_grid(data)
    data = find_unfound_grid(data)
    data.to_csv("improved_data.csv")
