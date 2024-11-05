from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
import requests
import pandas as pd
import msal
import time

class PowerBIDatasetRefreshOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        client_id,
        client_secret,
        tenant_name,
        workspace_id,
        dataset_id,
        timeout_seconds=3600,  # Default timeout is 30 minutes
        check_interval_seconds=300,  # Default check interval is 5 minutes
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_name = tenant_name
        self.workspace_id = workspace_id
        self.dataset_id = dataset_id
        self.timeout_seconds = timeout_seconds
        self.check_interval_seconds = check_interval_seconds

    def execute(self, context):
        authority_url = "https://login.microsoftonline.com/" + self.tenant_name
        scope = ["https://analysis.windows.net/powerbi/api/.default"]
        url = (
            "https://api.powerbi.com/v1.0/myorg/groups/"
            + self.workspace_id
            + "/datasets/"
            + self.dataset_id
            + "/refreshes?$top=1"
        )

        app = msal.ConfidentialClientApplication(
            self.client_id, authority=authority_url, client_credential=self.client_secret
        )
        result = app.acquire_token_for_client(scopes=scope)

        if "access_token" in result:
            access_token = result["access_token"]
            header = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {access_token}",
            }

            # Send a POST request to trigger the refresh
            api_call_trigger = requests.post(url=url, headers=header)

            if api_call_trigger.status_code == 202:
                self.log.info("Dataset refresh triggered successfully.")
            else:
                error_message = f"Failed to trigger dataset refresh. Status code: {api_call_trigger.status_code}"
                self.log.error(error_message)
                raise AirflowException(error_message)

            # Periodically check the status until timeout or completion
            start_time = time.time()
            while True:
                time.sleep(self.check_interval_seconds)
                api_call_status = requests.get(url=url, headers=header)
                result_status = api_call_status.json()["value"]

                df = pd.DataFrame(
                    result_status, columns=["requestId", "id", "refreshType", "startTime", "endTime", "status"]
                )
                df.set_index("id", inplace=True)

                if not df.empty:
                    status = df.loc[df.index[0], "status"]

                elapsed_time = time.time() - start_time
                if elapsed_time > self.timeout_seconds:
                    self.log.warning("Timeout waiting for dataset refresh completion.")
                    break

                if status == "Completed":
                    self.log.info("Dataset refresh is completed.")
                    break
                elif status == "Failed":
                    error_message = "Dataset refresh failed. Please check the error message."
                    self.log.error(error_message)
                    raise AirflowException(error_message)
                else:
                    self.log.info("Dataset refresh is still running. Status: %s", status)

            if status not in ["Completed", "Failed"]:
                self.log.warning("Dataset refresh status not conclusive after timeout.")
