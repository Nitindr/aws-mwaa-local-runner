from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
from datetime import datetime
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator

class SuccessHandlerOperator(BaseOperator):
    """
    Custom operator to handle failure by sending an SNS notification.

    :param target_arn: The ARN of the SNS topic to send the notification to.
    :type target_arn: str
    """

    @apply_defaults
    def __init__(
        self,
        target_arn,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.target_arn = target_arn

    def execute(self, context):
        task_instance = context['task_instance']
        dag_id = task_instance.dag_id
        ENV = Variable.get("Environment")
        link= Variable.get("Airflow_UI")

        success_emoji="✔️"

        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        message_subject = f'✔️Success Alert! for DAG "{dag_id}"'
        
        message = (
            f"**{message_subject}**\n\n"
            f"**Environment:** {ENV}\n\n"
            f"**Execution Time:** {timestamp} UTC\n\n"
            f"**DAG Details:**\n\n"
            f"  - DAG ID: **{dag_id}**\n\n"
            f"**More Info:** [Airflow UI]({link})"

        )

        sns_operator = SnsPublishOperator(
            task_id='send_sns_notification',
            target_arn=self.target_arn,
            message=message,
            subject=message_subject,
            aws_conn_id='aws_default',
            dag=self.dag,
        )

        sns_operator.execute(context)
