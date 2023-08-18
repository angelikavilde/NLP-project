"""Transforms the data by searching through affiliations and comparing the datasets"""

from datetime import datetime
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


def get_organisation_from_csv(institutes: DataFrame, row: str) -> str | None:
    """Finds an existing institutional organisation from affiliation data"""
    matched_organisation = extractOne(row, institutes["name"], scorer=partial_ratio, score_cutoff=80)
    if matched_organisation != None:
        return institutes[institutes["name"] == matched_organisation[0]].iloc[0]["grid_id"]


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
    institutes_df = pd.read_csv("institutes_data.csv")[["grid_id","name"]]
    data_frame["AffiliationMatchedGRID"] = data_frame["Organisation"].apply(
        lambda row: get_organisation_from_csv(institutes_df, row) if row != None else None)
    data_frame.to_csv("transformed_data.csv")


def find_organisation(row: Doc) -> str:
    """Finds an organisation in the affiliations"""
    for i in row.ents[::-1]:
       if i.label_ == "ORG" and len(i.text) > 7:
           # due to weird inconsistencies appropriate minimum length is added
           return i.text


if __name__=="__main__":
    time_now = datetime.now()
    print(f"Time started is {time_now}")
    data = pd.read_csv("data.csv")
    countries_data = pd.read_json("countries.json")["name"]
    get_new_columns(data, countries_data)
    time_after = datetime.now()
    remove("data.csv")
    difference_time = time_after-time_now
    print(f"Time difference is {difference_time.total_seconds()}s or {difference_time.seconds/60}m")

