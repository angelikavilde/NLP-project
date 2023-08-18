"""Retrieving data from xml to then process into a flat csv"""

import xml.etree.ElementTree as ET
from xml.etree.ElementTree import Element

import pandas as pd
import numpy as np
from numpy import ndarray


def find_articles(filename: str) -> tuple[Element]:
    """Finds all articles in a xml file"""
    tree = ET.parse(filename)
    root = tree.getroot()
    return root, root.findall("PubmedArticle")


def author_details(data: ndarray, author: Element, row_count: int) -> ndarray:
    """Finds author details"""
    data[row_count,5] = author.find("ForeName").text
    data[row_count,6] = author.find("LastName").text
    data[row_count,7] = author.find("Initials").text
    data[row_count,8] = author.find("ForeName").text + " " + author.find("LastName").text
    return data


def get_matrix_dimensions(root: Element, columns: list[str]) -> tuple[int]:
    """Returns max dimensions for the data matrix"""
    all_authors = root.findall("PubmedArticle/*/Article/AuthorList/Author")
    row_dim = 0
    for author in all_authors:
        author_list = author.findall("AffiliationInfo/Affiliation")
        row_dim += len(author_list)
        if len(author_list) == 0:
            row_dim += 1
    return row_dim, len(columns)


def article_information(data: ndarray, row_count: int, article: Element) -> ndarray | str:
    """Returns a value for each article identification or a string to be caught in the
    code to disregard the full row due to no article title found (as it is required)"""
    try:
        data[row_count,0] = article.find("*/Article/ArticleTitle").text
    except:
        return "No title found!"
    try:
        data[row_count,1] = article.find("*/PMID").text
    except:
        data[row_count,1] = None
    try:
        data[row_count,2] = ", ".join(keyword.text for keyword in 
                        article.findall("*/KeywordList/Keyword"))
    except:
        data[row_count,2] = None
    try:
        data[row_count,3] = ", ".join(code.attrib["UI"] for code in 
                article.findall(".//DescriptorName"))
    except:
        data[row_count,3] = None
    try:
        data[row_count,4] = article.find("*/Article/Journal/JournalIssue/PubDate/Year").text
    except:
        data[row_count,4] = None
    return data


def make_data_matrix(root: Element, columns: list[str], articles: Element) -> ndarray:
    """Creating a data-table"""
    data = np.empty(get_matrix_dimensions(root, columns), dtype='object')
    row_count = 0
    for article in articles:
        authors = article.findall("*/Article/AuthorList/Author")
        updated_data = article_information(data, row_count, article)
        if isinstance(updated_data, str):
            continue
        data = updated_data
        for author in authors:
            try:
                data = author_details(data, author, row_count)
            except:
                continue
            data = article_information(data, row_count, article)
            affiliation_info = author.findall("AffiliationInfo")
            if affiliation_info is not None:
                for index, affil in enumerate(affiliation_info):
                    affiliation = affil.find("Affiliation")
                    data[row_count, 9] = affiliation.text
                    identifiers = affil.findall("Identifier")
                    for identifier in identifiers:
                        if identifier.attrib["Source"] == "GRID":
                            data[row_count,10] = identifier.text
                    if index > 0:
                        data = author_details(data, author, row_count)
                        data = article_information(data, row_count, article)
                    row_count += 1
            else:
                row_count += 1
    return data


def create_csv_file(data: ndarray) -> None:
    """Creates a dataframe and saves into a csv file"""
    data_frame = pd.DataFrame(data=data, columns=columns)
    data_frame.drop_duplicates(ignore_index=True, inplace=True)
    data_frame = data_frame.dropna(subset=['Affiliations'])
    data_frame.to_csv("data.csv")
    

if __name__ == "__main__":
    columns = ["ArticleTitle","PMID","Keywords","MESH","Year","Name",
               "Surname","Initials","AuthorFullName","Affiliations","AffiliationGRID"]
    root, articles = find_articles("data.xml")
    data = make_data_matrix(root, columns, articles)
    create_csv_file(data)
