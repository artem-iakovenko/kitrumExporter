import os
import time
import json
from google.cloud import bigquery
from secret_manager import access_secret
from google.oauth2 import service_account

kitrum_bq_json = json.loads(access_secret("kitrum-cloud", "kitrum_bq"))
credentials = service_account.Credentials.from_service_account_info(kitrum_bq_json)
client = bigquery.Client(credentials=credentials, project=credentials.project_id)


class BigQuery:
    def __init__(self, table_id, view):
        self.table_id = table_id
        self.view = view

    def get_data_from_bq(self, sql_query):
        query_job = client.query(sql_query)
        bq_data = []
        for row in query_job.result():
            row_dict = {}
            for key in row.keys():
                row_dict[key] = row[key]
            bq_data.append(row_dict)
        return bq_data

    def insert_data(self):
        formatted_data = [
            {key.replace(" ", "_").lower().replace("/", "_"): value.replace(",", "") for key, value in item.items()}
            for item in self.view
        ]
        skip_keys = ['start_date_on_project', 'hired_by', 'dev_start_period']

        for item in formatted_data:
            for skip_key in skip_keys:
                try:
                    del item[skip_key]
                except:
                    pass

        query = f"TRUNCATE TABLE `{self.table_id}`"
        query_job = client.query(query)
        query_job.result()
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job = client.load_table_from_json(formatted_data, self.table_id, job_config=job_config)
        job.result()
        print(f"\n\t{len(formatted_data)} rows updated.")
