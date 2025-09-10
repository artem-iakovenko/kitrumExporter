import time
import requests
from datetime import date
from gsheet import GoogleSheet
from gbigquery import BigQuery
import csv
import io
from secret_manager import access_secret
bq_handler = BigQuery('kitrum-cloud.zoho_people.creds', None)
# zp_creds = bq_handler.get_data_from_bq("SELECT cookie, conreqcsr FROM `kitrum-cloud.zoho_people.creds`")

# conreqcsr = zp_creds[0]['conreqcsr']
# COOKIE = zp_creds[0]['cookie']
COOKIE = access_secret("kitrum-cloud", "zoho_people_cookie")
conreqcsr = access_secret("kitrum-cloud", "zoho_people_token")

# COOKIE = 'delugeBuilder=false; creator_cd="undefined:undefined:understand-workspace"; zabHMBucket=Ye22MRO; zabBucket=%7B%22mXFvaq9%22%3A%22%23%22%2C%22UCuaRWe%22%3A%220bf4246310f24fd79fd3a883a52d809e%22%2C%22WITIFrq%22%3A%22hn4D%22%2C%22RQRrq50%22%3A%22%23%22%2C%22MkV0ZQ4%22%3A%2204d5698f8dcb44ea8cbf9abd6a3d7895%22%2C%22Z5RV9t7%22%3A%22jm3X%22%2C%22UFOhDBP%22%3A%22o8kB%22%2C%22RWSPfoT%22%3A%22gkim%22%2C%220zr73lL%22%3A%22b63a4c4258c2440a9b27616838367222%22%2C%22lRqfhAZ%22%3A%22d7884d648bd04026a7614075d0bd04db%22%7D; zabSplit=%7B%22lRqfhAZ%22%3A%22d7884d648bd04026a7614075d0bd04db%22%2C%22returning%22%3Atrue%2C%22source%22%3A%22SEARCH%22%7D; zohocares-_zldp=YfEOFpfOAG%2Ftm8Lg3QPv8eNEyYXNMWRvkq8KpYgNukLPegrUXxc6ki%2FRRdRpih4MJDEB3Ylzdu0%3D; zohocares-_uuid=643be9d8-5028-4f1d-8e03-d1d42e2346b7_c942; showEditorLeftPane=undefined; zohosales-_zldp=%2B8WMNUXXNm2Wj5AfKSwKJYuKuVaVO2%2FfZhGXRlm773wAcLFnwtwt5IdpaSVVB52%2FDUil3S6c%2FWQ%3D; zohosales-_zldt=0e876035-e1d1-4360-b718-1e167feb8ec4-0; zalb_3309580ed5=04cf914e9b605292cfcb4e02227ec5d6; zalb_8b7213828d=540713a537d6cca885952c6c68c1dcd5; zalb_6570ccc01f=a13f7cfdbd8508b58e12ad76923fbd26; drecn=78c138b19f9ab7610f41d7f5777a87a10f61e7bba562f951ad3104da1b78fa1f66af2a626cbac50f8202c040118ceb205b68208736b152fb20880295c2d58300; embed1.zohobooks-_zldp=YfEOFpfOAG8R6WcIWQj%2B5Gx04m2zZRco%2B8OK9fhMDUJXbMkJwLla1VrzRovyj%2FiEyizI5qzYooI%3D; embed1.zohobooks-_zldt=16cb602c-3d44-4cc6-989a-c5343f18f396-1; zohocares-_zldt=aeb69b6e-a1d4-4898-bf88-fa3dd6441da4-2; _zpsid=0E4514127D88E178548EE136AF20F1B4; ZohoMarkRef="https://www.zoho.com/deluge/help/misc-statements/send-mail.html"; ZohoMarkSrc="google:recruit|google:zoho|google:zoho"; zps-tgr-dts=sc%3D791-expAppOnNewSession%3D%5B%5D-pc%3D6-sesst%3D1756387371636; JSESSIONID=1EB69BE6ECC84719261C3764B26ADAD8; _iamadt=369125d5a57de19a6c156eadf5facafeeaf8bff8f4b82c3d7290c644f71abb26b67214d802ce059c664beaa3d3bccba0; _iambdt=5b0f4dfe5b24efc9ddc115d5c3bdb5e25923316458c74aed69c4f27709996e6c8a3326282e4685c18e159e8532b8c3aea2f3f55e4f44f9333ed12fbe2d870b71; CSRF_TOKEN=431d55a53903519c82c923e01b04c45220366dc6f6e18b9a8c01f7c7973bdf220287a28978cfdb92e867db3caf2e84a684d0b03413b48216d8369ddc5310aa85; _zcsr_tmp=431d55a53903519c82c923e01b04c45220366dc6f6e18b9a8c01f7c7973bdf220287a28978cfdb92e867db3caf2e84a684d0b03413b48216d8369ddc5310aa85; CT_CSRF_TOKEN=431d55a53903519c82c923e01b04c45220366dc6f6e18b9a8c01f7c7973bdf220287a28978cfdb92e867db3caf2e84a684d0b03413b48216d8369ddc5310aa85; wms-tkp-token=713952135-780a2d14-19d00991ef648137c4b41c6b70648390; zalb_zid=648621720; com_chat_owner=1756450829463; com_avcliq_owner=1756450829464'
# conreqcsr = '431d55a53903519c82c923e01b04c45220366dc6f6e18b9a8c01f7c7973bdf220287a28978cfdb92e867db3caf2e84a684d0b03413b48216d8369ddc5310aa85'

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



