from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import requests
import pandas as pd
import msal


class PowerBIDatasetRefreshOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        client_id,
        client_secret,
        tenant_name,
        workspace_id,
        dataset_id,
        base_url="https://api.powerbi.com/v1.0/myorg/",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_name = tenant_name
        self.workspace_id = workspace_id
        self.dataset_id = dataset_id
        self.base_url = base_url
        self.headers = None

    def execute(self, context):
        authority_url = "https://login.microsoftonline.com/" + self.tenant_name
        scope = ["https://analysis.windows.net/powerbi/api/.default"]
        url = (
            self.base_url
            + "groups/"
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
            self.headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {access_token}",
            }
            api_call = requests.get(url=url, headers=self.headers)

            result = api_call.json()["value"]

            df = pd.DataFrame(
                result, columns=["requestId", "id", "refreshType", "startTime", "endTime", "status"]
            )
            df.set_index("id", inplace=True)

            if not df.empty:
                status = df.loc[df.index[0], "status"]
                if status == "Unknown":
                    self.log.info(
                        "Dataset is refreshing right now. Please wait until this refresh has finished to trigger a new one."
                    )
                elif status == "Disabled":
                    self.log.info("Dataset refresh is disabled. Please enable it.")
                elif status == "Failed":
                    self.log.info("Last dataset refresh failed. Please check the error message.")
                elif status == "Completed":
                    api_call = requests.post(url=url, headers=self.headers)
                    self.log.info("We triggered a dataset refresh.")
                else:
                    self.log.info(
                        "Not familiar with the status. Please check the documentation for status: %s", status
                    )
            else:
                self.log.info("No dataset refresh information available.")

        # Call the status tracking function
        self.get_pbi_refresh_status()

    def get_pbi_refresh_status(self):
        relative_url = (
            self.base_url + f"groups/{self.workspace_id}/datasets/{self.dataset_id}/refreshes"
        )
        response = requests.get(relative_url, headers=self.headers)

        refresh_status = response.json()
        latest_refresh = refresh_status["value"][0]
        status = latest_refresh["status"]
        self.log.info("Latest refresh status: %s", status)
