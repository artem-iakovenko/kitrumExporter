import time
import requests
from datetime import date
from gsheet import GoogleSheet
from gbigquery import BigQuery
import csv
import io

bq_handler = BigQuery('kitrum-cloud.zoho_people.creds', None)
zp_creds = bq_handler.get_data_from_bq("SELECT cookie, conreqcsr FROM `kitrum-cloud.zoho_people.creds`")

conreqcsr = zp_creds[0]['conreqcsr']
COOKIE = zp_creds[0]['cookie']

headers = {
    'Cookie': COOKIE,
    "User-Agent": 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
}


class LeaveExporter:
    def __init__(self):
        self.view = ""
        self.responses_lists = []
        self.years = []
        self.relevant_lines = []

    def export_leaves(self):
        for calendar_year in self.years:
            #employee_status = 3
            employee_status = -1
            post_data = {
                "isExportFromLeave": True,
                "ftype": "csv",
                "conreqcsr": conreqcsr,
                "viewID": "378942000000035695",
                "employeeStatus": employee_status,
                "fromDate": f"{calendar_year}-01-01",
                "toDate": f"{calendar_year}-12-31",
                "typeOfLeave": -1,
                "typeOfView": "all",
                "approvalStatus": "all"
            }
            response = requests.post("https://people.zoho.com/kitrum/ExportData.zp", headers=headers, data=post_data)
            content = response.content.decode('utf-8')
            csv_reader = csv.reader(io.StringIO(content))
            for row in csv_reader:
                if not row or row[0] == "ZOHO_LINK_ID":
                    continue
                leave_id = row[0]
                if leave_id in str(self.relevant_lines):
                    continue
                self.relevant_lines.append(row)


            # view_list = response.text.split("\n")
            # del view_list[0]
            # for line_item in view_list:
            #     if not line_item or line_item == "\r":
            #         continue
            #     leave_id = line_item.split(",")[0]
            #     if leave_id in str(self.relevant_lines):
            #         continue
            #     self.relevant_lines.append(line_item)
        # self.view = "\n".join(self.relevant_lines)

    def export(self):
        start_year = 2024
        current_year = date.today().year
        while True:
            self.years.append(start_year)
            if start_year == current_year:
                break
            start_year += 1
        self.export_leaves()
        sheet_handler = GoogleSheet("1L4BgMOcSa1_WghS1qW9VoA-NzPrCXh3fXh372upSpIA", "Zoho People - Export!A2:ZZ", self.relevant_lines)
        sheet_handler.remove_data_from_sheet()
        sheet_handler.insert_data()



