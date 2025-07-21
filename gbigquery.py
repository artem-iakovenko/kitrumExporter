import os
from google.cloud import bigquery
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'credentials/bq.json'
client = bigquery.Client()


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
            {key.replace(" ", "_").lower(): value.replace(",", "") for key, value in item.items()}
            for item in self.view
        ]
        query = f"TRUNCATE TABLE `{self.table_id}`"
        query_job = client.query(query)
        query_job.result()
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job = client.load_table_from_json(formatted_data, self.table_id, job_config=job_config)
        job.result()
        print(f"\n\t{len(formatted_data)} rows updated.")
