Please see https://airflow.apache.org/docs/apache-airflow/2.9.2/authoring-and-scheduling/plugins.html for information about creating Airflow plugin

Note that, per the above documentation, importing operators, sensors, hooks added in plugins via airflow.{operators,sensors,hooks}.<plugin_name> is no longer supported, and these extensions should just be imported as regular python modules.

# Custom Apache Airflow Operators

This repository contains custom Apache Airflow operators designed for various data workflows. These operators include:

- `PowerBIDatasetRefreshOperator`
- `FailureHandlerOperator`
- `SuccessHandlerOperator`
- `DoceboDataLoadOperator`

## Operators

### PowerBIDatasetRefreshOperator

This operator triggers a dataset refresh in Power BI and periodically checks the refresh status until completion or timeout.

#### Parameters

- `client_id`: The client ID for Power BI API authentication.
- `client_secret`: The client secret for Power BI API authentication.
- `tenant_name`: The tenant name for Power BI API authentication.
- `workspace_id`: The Power BI workspace ID containing the dataset.
- `dataset_id`: The Power BI dataset ID to be refreshed.
- `timeout_seconds`: Timeout in seconds for waiting for the dataset refresh to complete. Default is 3600 seconds (1 hour).
- `check_interval_seconds`: Interval in seconds to check the refresh status. Default is 300 seconds (5 minutes).

#### Example Usage

```python
from powerbi_dataset_refresh_operator import PowerBIDatasetRefreshOperator

refresh_operator = PowerBIDatasetRefreshOperator(
    task_id='refresh_powerbi_dataset',
    client_id='your-client-id',
    client_secret='your-client-secret',
    tenant_name='your-tenant-name',
    workspace_id='your-workspace-id',
    dataset_id='your-dataset-id',
    timeout_seconds=3600,
    check_interval_seconds=300
)
```

### FailureHandlerOperator

This operator handles task failures by sending an SNS notification.

Parameters
- `target_arn`: The ARN of the SNS topic to send the notification to.

#### Example Usage

```python
from success_handler_operator import FailureHandlerOperator

failure_handler = FailureHandlerOperator(
    task_id='handle_failure',
    target_arn='your-sns-topic-arn'
)
```
### SuccessHandlerOperator

This operator handles task successes by sending an SNS notification.

Parameters
- `target_arn`: The ARN of the SNS topic to send the notification to.

#### Example Usage
```python
from failure_handler_operator import SuccessHandlerOperator

success_handler = SuccessHandlerOperator(
    task_id='handle_success',
    target_arn='your-sns-topic-arn'
)
```
### DoceboDataLoadOperator

This operator loads data from the Docebo API and stores it in an S3 bucket. It supports multiple API endpoints and handles pagination.

Parameters
- `client_id`: The client ID for Docebo API authentication.
- `client_secret`: The client secret for Docebo API authentication.
- `grant_type`: The grant type for Docebo API authentication.
- `scope`: The scope for Docebo API authentication.
- `username`: The username for Docebo API authentication.
- `password`: The password for Docebo API authentication.
- `token_url`: The URL to obtain the access token.
- `api_endpoint`: The endpoint for the Docebo API.
- `s3_bucket`: The S3 bucket to store the data.
- `s3_secret_access_key`: The secret access key for S3.
- `s3_access_key_id`: The access key ID for S3.
- `s3_region`: The region for S3.
- `destination_folder`: The destination folder in the S3 bucket.
- `api_name`: The name of the Docebo API endpoint (e.g., 'user', 'enrollments', 'courses', 'reports').

#### Example Usage

```python
from ocebo_dataload_operator import DoceboDataLoadOperator

docebo_operator = DoceboDataLoadOperator(
    task_id='load_docebo_data',
    client_id='your-client-id',
    client_secret='your-client-secret',
    grant_type='your-grant-type',
    scope='your-scope',
    username='your-username',
    password='your-password',
    token_url='your-token-url',
    api_endpoint='your-api-endpoint',
    s3_bucket='your-s3-bucket',
    s3_secret_access_key='your-s3-secret-access-key',
    s3_access_key_id='your-s3-access-key-id',
    s3_region='your-s3-region',
    destination_folder='your-destination-folder',
    api_name='your-api-name'
)
```
