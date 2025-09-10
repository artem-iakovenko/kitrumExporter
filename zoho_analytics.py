import requests
import time
from gsheet import GoogleSheet
from gbigquery import BigQuery
from secret_manager import access_secret
import json

analytics_secrets = json.loads(access_secret("kitrum-cloud", "zoho_analytics"))

def refresh_access_token():
    response = requests.post(f"https://accounts.zoho.com/oauth/v2/token?refresh_token={analytics_secrets['refresh_token']}&client_id={analytics_secrets['client_id']}&client_secret={analytics_secrets['client_secret']}&grant_type=refresh_token")
    return response.json()['access_token']


ACCESS_HEADERS = {
    "ZANALYTICS-ORGID": analytics_secrets['org_id'],
    "Authorization": f"Zoho-oauthtoken {refresh_access_token()}"
}


class ViewExporter:
    def __init__(self, config):
        self.config = config
        self.view = []

    def export_view(self):
        timeout_time = 10
        view_id = self.config['view_id']
        if self.config['format'] == "csv":
            request_config = "{'responseFormat': 'csv'}"
        elif self.config['format'] == "json":
            request_config = "{'responseFormat': 'json'}"
        else:
            return "Unsupported Format"

        create_job = requests.get(
            f"https://analyticsapi.zoho.com/restapi/v2/bulk/workspaces/{analytics_secrets['workspace_id']}/views/{view_id}/data?CONFIG={request_config}",
            headers=ACCESS_HEADERS
        )
        print(f"\t{create_job.json()}")
        job_id = create_job.json()['data']['jobId']
        while True:
            get_job_status = requests.get(
                f"https://analyticsapi.zoho.com/restapi/v2/bulk/workspaces/{analytics_secrets['workspace_id']}/exportjobs/{job_id}",
                headers=ACCESS_HEADERS
            )
            job_status = get_job_status.json()['data']['jobStatus']
            if job_status == 'JOB COMPLETED':
                download_data = requests.get(
                    f"https://analyticsapi.zoho.com/restapi/v2/bulk/workspaces/{analytics_secrets['workspace_id']}/exportjobs/{job_id}/data",
                    headers=ACCESS_HEADERS
                )
                if self.config['format'] == "csv":
                    self.view = download_data.text
                elif self.config['format'] == "json":
                    self.view = download_data.json()['data']
                break
            print(f"\tCurrent Status: {job_status}.Waiting {timeout_time} seconds before new status check")
            time.sleep(timeout_time)

    def export(self):
        self.export_view()
        if not self.view:
            return "No data extracted from view"
        if self.config['destination'] == "google_sheet":
            sheet_handler = GoogleSheet(self.config['sheet_id'], self.config['range'], self.view)
            sheet_handler.remove_data_from_sheet()
            sheet_handler.insert_data_additional()
        elif self.config['destination'] == "bigquery":
            bq_handler = BigQuery(self.config['table_id'], self.view)
            bq_handler.insert_data()


