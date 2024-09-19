import requests
import csv
import time
import json
from datetime import datetime, timedelta
import os

# Load Configuration from JSON file
def load_config():
    config_path = 'config/config.json'
    with open(config_path, 'r') as file:
        return json.load(file)

config = load_config()

# AWS and Northbeam Configuration
S3_BUCKET_NAME = config['aws']['S3_BUCKET_NAME']
AWS_ACCESS_KEY = config['aws']['AWS_ACCESS_KEY']
AWS_SECRET_KEY = config['aws']['AWS_SECRET_KEY']
AWS_REGION = config['aws']['AWS_REGION']
S3_FOLDER_NAME = config['aws']['S3_folder_name']

API_URL = config['northbeam']['API_URL']
DATA_CLIENT_ID = config['northbeam']['DATA_CLIENT_ID']
API_KEY = config['northbeam']['API_KEY']
ATTRIBUTION_MODEL = config['northbeam']['ATTRIBUTION_MODEL']
PLATFORMS = config['northbeam']['PLATFORMS']
CLIENT_NAME = config['northbeam']['CLIENT_NAME']
META_DATA_FILE = config['northbeam']['META_DATA_FILE']

HEADERS = {
    'Authorization': API_KEY,
    'Data-Client-ID': DATA_CLIENT_ID,
    'Content-Type': 'application/json',
}

# Metadata Handling
class MetadataManager:
    def __init__(self, meta_file):
        self.meta_file = meta_file
        self._initialize_metadata()

    def _initialize_metadata(self):
        if not os.path.exists(self.meta_file):
            with open(self.meta_file, 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(['last_fetched_date', 'success'])

    def get_last_fetched_date(self):
        try:
            with open(self.meta_file, 'r') as file:
                reader = csv.DictReader(file)
                rows = list(reader)
                for row in reversed(rows):
                    if row.get('success') == 'SUCCESS':
                        return row['last_fetched_date']
        except Exception as e:
            print(f"Error reading metadata file: {e}")
        return None

    def update_metadata(self, date_fetched, success):
        with open(self.meta_file, 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([date_fetched, success])

# Date Utility Functions
def determine_fetch_dates(metadata_manager):
    last_fetched = metadata_manager.get_last_fetched_date()

    if last_fetched:
        period_start = datetime.strptime(last_fetched, '%Y-%m-%d') + timedelta(days=1)
    else:
        period_start = datetime(datetime.now().year, 1, 1)

    period_end = period_start
    return period_start, period_end

def format_date_for_api(date_obj):
    return date_obj.strftime('%Y-%m-%dT%H:%M:%SZ')

# API Interaction
class NorthbeamAPI:
    def __init__(self, api_url, headers):
        self.api_url = api_url
        self.headers = headers

    def initiate_export(self, payload):
        response = requests.post(self.api_url, headers=self.headers, json=payload)
        return response.json()

    def check_export_status(self, export_id):
        status_url = f"https://api.northbeam.io/v1/exports/data-export/result/{export_id}"
        status_response = requests.get(status_url, headers=self.headers)
        return status_response.json()

    def download_export_file(self, file_url, csv_filename):
        export_file = requests.get(file_url)
        with open(csv_filename, 'wb') as file:
            file.write(export_file.content)
        print(f"CSV file '{csv_filename}' has been downloaded successfully.")

# Export Functionality
class ExportManager:
    def __init__(self, api, metadata_manager):
        self.api = api
        self.metadata_manager = metadata_manager

    def create_payload(self, period_start, period_end):
        return {
            "level": "platform",
            "time_granularity": "DAILY",
            "period_type": "FIXED",
            "period_options": {
                "period_starting_at": format_date_for_api(period_start),
                "period_ending_at": format_date_for_api(period_end + timedelta(hours=23, minutes=59, seconds=59))
            },
            "breakdowns": [
                {
                    "key": "Platform (Northbeam)",
                    "values": PLATFORMS
                }
            ],
            "options": {
                "export_aggregation": "BREAKDOWN",
                "remove_zero_spend": False,
                "aggregate_data": True,
                "include_ids": False,
                "include_kind_and_platform": False
            },
            "attribution_options": {
                "attribution_models": [ATTRIBUTION_MODEL],
                "accounting_modes": ["accrual"],
                "attribution_windows": ["1"]
            },
            "metrics": [
                {"id": "spend", "label": "spend_amount"},
                {"id": "aov", "label": "AOV"},
                {"id": "googleClicks", "label": "Google_Clicks"},
                {"id": "impressions", "label": "Imprs"},
                {"id": "metaLinkClicks", "label": "FB_Link_Clicks_Default"},
                {"id": "roas", "label": "ROAS"}
            ]
        }

    def fetch_export_data(self, period_start, period_end):
        payload = self.create_payload(period_start, period_end)
        data = self.api.initiate_export(payload)
        export_id = data.get("id")

        if export_id:
            return self.poll_export_status(export_id, period_start)
        else:
            print(f"Failed to initiate export. Error: {data}")
            return None

    def poll_export_status(self, export_id, period_start):
        while True:
            status_data = self.api.check_export_status(export_id)

            if status_data.get("status") == "SUCCESS":
                file_url = status_data.get("result")[0]
                csv_filename = f"{CLIENT_NAME}_spend_{period_start.strftime('%Y-%m-%d')}.csv"
                self.api.download_export_file(file_url, csv_filename)
                self.metadata_manager.update_metadata(period_start.strftime('%Y-%m-%d'), 'SUCCESS')
                return csv_filename  # Return the file path here
            elif status_data.get("status") == "FAILED":
                print("Export failed.")
                self.metadata_manager.update_metadata(period_start.strftime('%Y-%m-%d'), 'FAILED')
                return None
            else:
                print("Export in progress, checking again in 10 seconds...")
                time.sleep(10)

def main():
    metadata_manager = MetadataManager(META_DATA_FILE)
    northbeam_api = NorthbeamAPI(API_URL, HEADERS)

    period_start, period_end = determine_fetch_dates(metadata_manager)

    # Debugging information
    print(f"Period Start: {period_start.strftime('%Y-%m-%d')}, Period End: {period_end.strftime('%Y-%m-%d')}")

    # Export Manager to handle fetching and downloading
    export_manager = ExportManager(northbeam_api, metadata_manager)
    csv_file_name = export_manager.fetch_export_data(period_start, period_end)

    return csv_file_name  # Return the CSV file path when called for s3

if __name__ == "__main__":
    csv_path = main()
    print(f"CSV file saved at: {csv_path}")
