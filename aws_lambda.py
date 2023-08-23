"""AWS Lambda function file"""

from os import remove

import pandas as pd

from find_file import get_bucket_connection, search_for_new_files
from find_file import TIME_NOW, download_new_file
from extract import find_articles, make_data_matrix, create_csv_file
from transform import get_new_columns


def lambda_handler(event = None, context = None) -> str:
    """Returns if data was indeed found"""
    columns = ["ArticleTitle","PMID","Keywords","MESH","Year","Name",
               "Surname","Initials","AuthorFullName","Affiliations","AffiliationGRID"]
    connection = get_bucket_connection()
    bucket_name = "sigma-pharmazer-input"
    all_files = search_for_new_files(connection, bucket_name)
    download_new_file(connection, bucket_name, all_files)
    try:
        root, articles = find_articles("/tmp/data.xml")
        data = make_data_matrix(root, columns, articles)
        create_csv_file(data, columns)
        data = pd.read_csv("/tmp/data.csv")
        countries_data = pd.read_json("countries.json")["name"]
        get_new_columns(data, countries_data)
        remove("/tmp/data.csv")
        time = "-".join([str(TIME_NOW.day),str(TIME_NOW.hour),str(TIME_NOW.minute)])
        file_name = f"angela/data_{time}.csv"
        connection.upload_file("/tmp/transformed_data.csv", "sigma-pharmazer-output", file_name)
        remove("/tmp/transformed_data.csv")
        state = "Relevant file was found!"
    except FileNotFoundError as e:
        state = "Relevant file was NOT found!"
        print(e)
    return {"State": state}


if __name__ == "__main__":
    print(lambda_handler())
