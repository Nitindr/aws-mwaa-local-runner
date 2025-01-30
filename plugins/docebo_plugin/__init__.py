from airflow.plugins_manager import AirflowPlugin
from docebo_plugin.operators.docebo_dataload_operator import DoceboDataLoadOperator
from docebo_plugin import docebo_config


class DoceboPlugin(AirflowPlugin):
    name = 'docebo_plugin'
    hooks = []
    operators = [DoceboDataLoadOperator]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
