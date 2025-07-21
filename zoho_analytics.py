import requests
import time
from gsheet import GoogleSheet
from gbigquery import BigQuery
from credentials.secrets import REFRESH_TOKEN, CLIENT_SECRET, CLIENT_ID, WORKSPACE_ID, ORG_ID

# REFRESH_TOKEN = "1000.4b5d70847d9be2ca4b4fd3c126359bf6.07966060ce621d7c534902afcbc2761f"
# CLIENT_ID = "1000.T5VGK6CRDD7W6W8MO9QPZD6ZIF3TSG"
# CLIENT_SECRET = "087ee51ba48ed0ce72294457d025eba49db635bb1e"
# WORKSPACE_ID = "2068366000001655001"
# ORG_ID = "696045712"


def refresh_access_token():
    response = requests.post(f"https://accounts.zoho.com/oauth/v2/token?refresh_token={REFRESH_TOKEN}&client_id={CLIENT_ID}&client_secret={CLIENT_SECRET}&grant_type=refresh_token")
    return response.json()['access_token']


ACCESS_HEADERS = {
    "ZANALYTICS-ORGID": ORG_ID,
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
            f"https://analyticsapi.zoho.com/restapi/v2/bulk/workspaces/{WORKSPACE_ID}/views/{view_id}/data?CONFIG={request_config}",
            headers=ACCESS_HEADERS
        )
        print(f"\t{create_job.json()}")
        job_id = create_job.json()['data']['jobId']
        while True:
            get_job_status = requests.get(
                f"https://analyticsapi.zoho.com/restapi/v2/bulk/workspaces/{WORKSPACE_ID}/exportjobs/{job_id}",
                headers=ACCESS_HEADERS
            )
            job_status = get_job_status.json()['data']['jobStatus']
            if job_status == 'JOB COMPLETED':
                download_data = requests.get(
                    f"https://analyticsapi.zoho.com/restapi/v2/bulk/workspaces/{WORKSPACE_ID}/exportjobs/{job_id}/data",
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


