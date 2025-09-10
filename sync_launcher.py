import time

from zoho_analytics import ViewExporter
from zoho_people import LeaveExporter


def export_analytics():
    views = [
        {"view_id": "2068366000042926211", "format": "csv", "destination": "google_sheet", "sheet_id": "1fhDEWThVCeFHF9HaVzpDtNv5VfVbeSFHJyG6cnqKBaA", "range": "Timelogs"},
        {"view_id": "2068366000047333565", "format": "csv", "destination": "google_sheet",
         "sheet_id": "1fhDEWThVCeFHF9HaVzpDtNv5VfVbeSFHJyG6cnqKBaA", "range": "HoursByProject!A1:F"},
        {"view_id": "2068366000047333836", "format": "csv", "destination": "google_sheet",
         "sheet_id": "1fhDEWThVCeFHF9HaVzpDtNv5VfVbeSFHJyG6cnqKBaA", "range": "Overtimes!A1:F"},
        {"view_id": "2068366000047654014", "format": "csv", "destination": "google_sheet",
         "sheet_id": "1fhDEWThVCeFHF9HaVzpDtNv5VfVbeSFHJyG6cnqKBaA", "range": "Overhead Export!A1:C"},
        {"view_id": "2068366000047654850", "format": "csv", "destination": "google_sheet",
         "sheet_id": "1fhDEWThVCeFHF9HaVzpDtNv5VfVbeSFHJyG6cnqKBaA", "range": "Unused Vacations Requests!A1:I"},
        {"view_id": "2068366000035817388", "format": "json", "destination": "bigquery",
         "table_id": "kitrum-cloud.zoho_analytics.project_gross_calcuations"},
    ]

    for view in views:
        print(f"\nCurrent View: {view['view_id']}")
        view_exporter = ViewExporter(view)
        view_exporter.export()


def export_leaves():
    leave_exporter = LeaveExporter()
    leave_exporter.export()


def main():
    print("\n\n===================== Exporting Leaves from Zoho People =====================")
    export_leaves()
    print("===================== Exporting Views from Zoho Analytics =====================")
    export_analytics()



main()
