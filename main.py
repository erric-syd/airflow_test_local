# bigQuery
import bq_helper
from bq_helper import BigQueryHelper
from google.oauth2 import service_account

# Pipeline
from airflow.models import Variable

# General
from datetime import datetime
import os
import pandas as pd
import numpy as np
import json


def run_StackoverflowDatasetEtl():

    bq_conn_id = "my_gcp_conn"

    dt_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(f'START {dt_now}, {os.getcwd()}')

    # # Still dunno how to import variable environment to airflow on local. Hmm...
    # Environment Variables
    # variable = Variable.get("GENERAL_ENV", deserialize_json=True)
    # var_google_credentials = json.load(Variable.get("GOOGLE_APPLICATION_CREDENTIALS", deserialize_json=True))
    # var_db_url_tes = Variable.get("GENERAL_ENV", deserialize_json=True)
    # google_credentials = var_google_credentials['GOOGLE_APPLICATION_CREDENTIALS']
    # db_url_tes = var_db_url_tes['DB_URL_TES']
    # google_credentials = variable['GOOGLE_APPLICATION_CREDENTIALS']
    # db_url_tes = variable['DB_URL_TES']
    google_credentials = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    db_url_tes = os.environ['DB_URL_TES']



    # -------------------------------------------------------------------------------------------- #



    # Get the GOOGLE_APPLICATION_CREDENTIALS from GCP Service Accounts etl_test > Keys
    credentials = service_account.Credentials.from_service_account_file(
        google_credentials,
        scopes=['https://www.googleapis.com/auth/cloud-platform'])
    stackOverflow = bq_helper.BigQueryHelper(active_project="bigquery-public-data", dataset_name="stackoverflow")

    # print list of tables
    # bq_assistant = BigQueryHelper("bigquery-public-data", "stackoverflow")
    # print(bq_assistant.list_tables())

    # Get data from kaggle, stack overflow data (bigQuery Dataset)
    # https://www.kaggle.com/stackoverflow/stackoverflow?select=users
    # Solved error by pip install pyarrow==1.0.1 (correct version)
    q = """
        SELECT
            users.display_name,
            users.location,
            users.reputation
        FROM
            `bigquery-public-data.stackoverflow.users` users
        WHERE
            users.creation_date BETWEEN '2008-07-31' and '2008-08-01 23:59:59.997'
        LIMIT 100
    """
    df = stackOverflow.query_to_pandas_safe(q)

    # In case need for .csv
    # df.to_csv("result.csv", index=False)
    # df = pd.read_csv('result.csv')

    # Group by location, get count of display_name
    df_location = df.groupby(['location'])['display_name'].count().reset_index()
    df_location.sort_values(by=['display_name'], ascending=False, inplace=True)
    df_location.rename(columns={'display_name': 'count_location'}, inplace=True)

    # Group by reputation, get max reputation from each location
    df_reputation = df.groupby(['location']).agg({'reputation': np.max}).reset_index()
    df_reputation.sort_values(by=['reputation'], ascending=False, inplace=True)

    # Merge df_reputation & df to get the name of the max_reputation users
    df_reputation = df_reputation.merge(
        df.loc[:, ['location', 'reputation', 'display_name']],
        how='left', left_on=['location', 'reputation'], right_on=['location', 'reputation'])

    # If there are more than 1 user with the same location & max_reputation value, merge into one column.
    df_reputation = df_reputation.groupby(['location', 'reputation'])['display_name'].agg(
        lambda x: ', '.join(x.dropna())).reset_index()
    df_reputation.rename(columns={'reputation': 'max_reputation'}, inplace=True)

    # Merge count of display_name & max_reputation from each location
    result = df_location.merge(df_reputation, how='left', left_on=['location'], right_on=['location'])

    # Assign created_date
    result['created_date'] = dt_now

    # Save to database
    result.to_sql("users_summary_detail_etl_test", db_url_tes, if_exists="replace", index=False)
    print(f'Save to DB Successfully, {dt_now}')

    print(f'DONE {dt_now}')