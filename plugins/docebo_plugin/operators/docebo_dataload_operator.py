from concurrent.futures import ThreadPoolExecutor, as_completed
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import requests
import pandas as pd
import boto3
import io
import time
from datetime import datetime
import os
import multiprocessing
import threading
from docebo.config import docebo_config

class DoceboDataLoadOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        client_id,
        client_secret,
        grant_type,
        scope,
        username,
        password,
        token_url,
        api_endpoint,
        s3_bucket,
        s3_secret_access_key,
        s3_access_key_id,
        s3_region,
        destination_folder,
        api_name,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.client_id = client_id
        self.client_secret = client_secret
        self.grant_type = grant_type
        self.scope = scope
        self.username = username
        self.password = password
        self.api_endpoint = api_endpoint
        self.token_url = token_url
        self.s3_bucket = s3_bucket
        self.aws_access_key_id = s3_access_key_id
        self.aws_secret_access_key = s3_secret_access_key
        self.region_name = s3_region
        self.api_name = api_name
        self.destination_folder = destination_folder
        self.api_call_count = 0

    def get_access_token(self):
        files = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": self.grant_type,
            "scope": self.scope,
            "username": self.username,
            "password": self.password,
        }

        response = requests.post(url=self.token_url, data=files)
        response.raise_for_status()
        token_data = response.json()
        return token_data["access_token"]

    def get_report_id(self, api_base_url, headers):
        params = {"count": 500}
        response = requests.get("https://onetrustlearning.docebosaas.com/analytics/v1/reports", headers=headers, params=params)
        response.raise_for_status()
        reports_data = response.json()
        id_report = None

        for item in reports_data.get('data', []):
            if item.get('name') == docebo_config.report_name:
                id_report = item.get('idReport')
                break

        return id_report

    def start_report_export(self, api_base_url, headers, report_id):
        api = "https://onetrustlearning.docebosaas.com"
        response = requests.get(f"{api}/analytics/v1/reports/{report_id}/export/csv", headers=headers)
        response.raise_for_status()
        export_id = response.json()
        return export_id

    def get_report_data_paginated(self, api_base_url, headers, report_id, export_id, page):
        all_data = []
        page_size = 1000
        next_token = None

        while True:
            params = {"pageSize": page_size, "page": page}
            if next_token:
                params['nextToken'] = next_token

            api = "https://onetrustlearning.docebosaas.com"
            response = requests.get(
                f"{api}/analytics/v1/reports/{report_id}/exports/{export_id}/results",
                headers=headers,
                params=params
            )
            self.api_call_count += 1
            if response.status_code == 200:
                data = response.json()
                all_data.extend(data['data'])
                self.log.info(f"Fetched {len(data['data'])} records for page {page}")

                next_token = data.get('nextToken')
                if not next_token:
                    break

                page += 1

            elif response.status_code == 400:
                #self.log.info("Received 400 error, retrying in 5 seconds...")
                time.sleep(5)

            else:
                response.raise_for_status()

        return all_data, next_token

    def fetch_data(self, page):
        params = {"page_size": 200, "page": page}
        thread_name = threading.current_thread().name
        response = requests.get(self.api_endpoint, headers=self.headers, params=params)
        self.api_call_count += 1
        if response.status_code == 200:
            data = response.json()["data"]
            return data["items"], data.get("has_more_data", False), page, thread_name
        else:
            self.log.error(f"[{thread_name}] Failed to retrieve data for page {page}: {response.status_code}")
            return [], False, page, thread_name

    def execute(self, context):
        access_token = self.get_access_token()
        self.headers = {"Authorization": f"Bearer {access_token}"}

        combined_df = pd.DataFrame()
        page = 1
        cpu_count = multiprocessing.cpu_count()
        self.log.info(f"Using {cpu_count} workers based on CPU count")

        if self.api_name in ['user', 'enrollments', 'courses']:
            has_more_data = True

            while has_more_data:
                with ThreadPoolExecutor(max_workers=cpu_count) as executor:
                    futures = []
                    for i in range(cpu_count):
                        futures.append(executor.submit(self.fetch_data, page + i))

                    has_more_data = False
                    for future in as_completed(futures):
                        api_data, more_data, page_completed, thread_name = future.result()
                        self.log.info(f"[{thread_name}] Page {page_completed} completed")
                        if api_data:
                            df = pd.DataFrame(api_data)
                            if self.api_name == "user":
                                filtered_df = df[~df['username'].str.contains('onetrust', case=False)]
                            elif self.api_name == "enrollments":
                                filtered_df = df[~df['username'].str.contains('onetrust', case=False)]
                            elif self.api_name == "courses":
                                filtered_df = df
                            combined_df = pd.concat([combined_df, filtered_df], ignore_index=True)
                        has_more_data = has_more_data or more_data
                    page += cpu_count

            self.log.info(f"Total API calls made: {self.api_call_count}")

            if not combined_df.empty:
                self.log.info("Processing and writing data to S3")
                # current_time = datetime.now().strftime("%Y-%m-%d")
                file_name = f"{self.api_name}.json"
                s3_key = os.path.join(self.destination_folder, file_name)
                s3_client = boto3.client(
                    's3',
                    aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key=self.aws_secret_access_key,
                    region_name=self.region_name
                )
                with io.StringIO() as json_buffer:
                    combined_df.to_json(json_buffer, orient='records', lines=True)
                    s3_client.put_object(
                        Bucket=self.s3_bucket,
                        Key=s3_key,
                        Body=json_buffer.getvalue()
                    )
                del combined_df


        elif self.api_name == "reports":
            self.log.info("Extracting the report ID and export ID")
            api_base_url = "https://onetrustlearning.docebosaas.com"
            report_end_point = self.api_endpoint
            report_id = self.get_report_id(report_end_point, self.headers)
            if report_id:
                export_url = self.start_report_export(api_base_url, self.headers, report_id)
                export_id = export_url['data']['executionId']

                self.log.info(f"Report ID: {report_id}, Export ID: {export_id}")
                additional_data_combined = []
                next_token = None
                while True:
                    additional_data, next_token = self.get_report_data_paginated(api_base_url, self.headers, report_id, export_id, page)
                    additional_data_combined.extend(additional_data)
                    #self.log.info(f"Processed page {page} for report data")

                    if not next_token:
                        break
                    page += 1

                additional_df = pd.DataFrame(additional_data_combined)
                
                file_name = f"{self.api_name}.csv"
                
                additional_s3_key = os.path.join(self.destination_folder, file_name)
                s3_client = boto3.client(
                    's3',
                    aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key=self.aws_secret_access_key,
                    region_name=self.region_name
                )
                with io.StringIO() as additional_buffer:
                    additional_df.to_csv(additional_buffer, index=False)
                    s3_client.put_object(
                        Bucket=self.s3_bucket,
                        Key=additional_s3_key,
                        Body=additional_buffer.getvalue()
                    )
                del additional_df, additional_data_combined

            else:
                self.log.error("Failed to find report ID")


        else:
            self.log.error(f"Unsupported API name: {self.api_name}")

        self.log.info(f"Total API calls made: {self.api_call_count}")
