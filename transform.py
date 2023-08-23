"""Transforms the data by searching through affiliations and comparing the datasets"""

from os import remove

import pandas as pd
import spacy
from spacy.tokens.doc import Doc
from pandas import DataFrame
from rapidfuzz.fuzz import partial_ratio
from rapidfuzz.process import extractOne


def find_country(row: str, countries_df: DataFrame):
    """Finds a country in the affiliation name"""
    if row is None:
        return
    for country in countries_df:
        if country in row:
            return country


def get_organisation_from_csv(institutes: DataFrame, organisation: str, country: str, cache: dict) -> str | None:
    """Finds an existing institutional organisation from affiliation data"""
    if organisation in cache:
        return cache[organisation]
    if country is not None:
        institute_names = institutes[institutes["country"] == country]
    if country is None or institute_names.isnull:
        institute_names = institutes["name"]
    matched_organisation = extractOne(organisation, institute_names, scorer=partial_ratio, score_cutoff=80)
    if matched_organisation is not None:
        returned_value = institutes[institutes["name"] == matched_organisation[0]].iloc[0]["grid_id"]
        cache[organisation] = returned_value
        return returned_value


def get_new_columns(data_frame: DataFrame, countries_df: DataFrame) -> None:
    """Adds new retrieved data in the datafile"""
    nlp = spacy.load("en_core_web_sm", disable=["tagger", "parser", "textcat", "senter", "lemmatizer"])
    data_frame["Organisation"] = list(nlp.pipe(data_frame["Affiliations"], n_process=10, batch_size=1000))
    data_frame["Organisation"] = data_frame["Organisation"].apply(find_organisation)
    email_expression = "((?:(?:[a-z0-9_-]+\.)?)+[a-z0-9_-]+@[a-z0-9_-]+\.[a-z]+(?:\.[a-z]+)?)"
    data_frame["Email"] = data_frame["Affiliations"].str.lower().str.extract(email_expression)
    data_frame["Country"] = data_frame["Affiliations"].apply(lambda row: find_country(row, countries_df))
    zipcode_expression = "([A-Z]{1,2}\d{1,2}[A-Z]? ?\d[A-Z]{2}|\b\d{5}\b|\d{3}-\d{4}|\b\d{6}\b)"
    data_frame["ZipCode"] = data_frame["Affiliations"].str.upper().str.extract(zipcode_expression)
    institutes_df = pd.read_csv("institutes_data.csv")[["grid_id","name"]].set_index("grid_id")
    adresses_df = pd.read_csv("addresses_data.csv", low_memory=False)[["grid_id", "country"]].set_index("grid_id")
    institutes_combined_df = institutes_df.join(adresses_df)
    institutes_combined_df["country"].replace({"United Kingdom": "UK", "United States": "USA"}, inplace=True)
    institutes_combined_df = institutes_combined_df.reset_index()
    cache_dict = dict()
    data_frame["AffiliationMatchedGRID"] = data_frame.apply(
        lambda row: get_organisation_from_csv(institutes_combined_df, row["Organisation"],
                    row["Country"], cache_dict) if row["Organisation"] is not None else None, axis=1)
    data_frame.to_csv("transformed_data.csv")

def find_organisation(row: Doc) -> str:
    """Finds an organisation in the affiliations"""
    for i in row.ents[::-1]:
       if i.label_ == "ORG" and len(i.text) > 7:
           # due to weird inconsistencies appropriate minimum length is added
           return i.text


if __name__=="__main__":
    data = pd.read_csv("data.csv")
    countries_data = pd.read_json("countries.json")["name"]
    get_new_columns(data, countries_data)
    remove("data.csv")

