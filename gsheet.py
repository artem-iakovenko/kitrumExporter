from google.oauth2 import service_account
from googleapiclient.discovery import build
import csv
import io

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']


class GoogleSheet:
    def __init__(self, spreadsheet_id, sheet_range, view):
        self.spreadsheet_id = spreadsheet_id
        self.sheet_range = sheet_range
        self.view = view
        self.creds = service_account.Credentials.from_service_account_file(
            'credentials/sheets.json', scopes=SCOPES
        )
        self.service = build('sheets', 'v4', credentials=self.creds)

    def remove_data_from_sheet(self):
        sheet = self.service.spreadsheets()
        if 'values' not in sheet.values().get(spreadsheetId=self.spreadsheet_id, range=self.sheet_range).execute():
            return
        sheet.values().clear(
            spreadsheetId=self.spreadsheet_id,
            range=self.sheet_range
        ).execute()

    def insert_data(self):
        # csv_reader = csv.reader(io.StringIO(self.view))
        # values = list(csv_reader)
        body = {
            'values': self.view
        }
        result = self.service.spreadsheets().values().update(
            spreadsheetId=self.spreadsheet_id,
            range=self.sheet_range,
            valueInputOption="USER_ENTERED",
            body=body
        ).execute()
        print(f"\n\t{result.get('updatedRows')} rows updated.")

    def insert_data_additional(self):
        csv_reader = csv.reader(io.StringIO(self.view))
        values = list(csv_reader)
        body = {
            'values': values
        }
        result = self.service.spreadsheets().values().update(
            spreadsheetId=self.spreadsheet_id,
            range=self.sheet_range,
            valueInputOption="USER_ENTERED",
            body=body
        ).execute()
        print(f"\n\t{result.get('updatedRows')} rows updated.")