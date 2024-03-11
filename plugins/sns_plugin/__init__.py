from airflow.plugins_manager import AirflowPlugin
from sns_plugin.operators.success_handler_operator import SuccessHandlerOperator
from sns_plugin.operators.failure_handler_operator import FailureHandlerOperator


class SnsPlugin(AirflowPlugin):
    name = 'sns_plugin'
    hooks = []
    operators = [FailureHandlerOperator, SuccessHandlerOperator]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []

