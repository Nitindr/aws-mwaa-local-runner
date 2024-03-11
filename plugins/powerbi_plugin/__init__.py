from airflow.plugins_manager import AirflowPlugin
from powerbi_plugin.operators.powerbi_dataset_refresh_operator import PowerBIDatasetRefreshOperator


class PowerBIPlugin(AirflowPlugin):
    name = 'powerbi_plugin'
    hooks = []
    operators = [PowerBIDatasetRefreshOperator]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
