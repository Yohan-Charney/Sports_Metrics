import requests
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
import os


def send_slack_alert(context):
    import requests
    dag_id = context.get('dag').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')

    message = {
        "text": f":red_circle: *DAG Failed*\n*DAG*: {dag_id}\n*Task*: {task_id}\n*Date*: {execution_date}"
    }

    webhook_url =  Variable.get("slack_webhook_url")
    requests.post(webhook_url, json=message)


def execute_n8n_workflow():
    url = Variable.get("n8n_webhook_url")
    response = requests.get(url)
    response.raise_for_status()
    logging.info(response.json())
    return response.json()


default_args = {
    'owner': 'yoan.mboulou',
    'depends_on_past': False,
    'email': [Variable.get("alert_email", default_var="")],
    'email_on_failure': True,
    'retries': 3,
    'retry_delay': timedelta(minutes = 5),
    'retry_exponential_backoff': True,
    'on_failure_callback': send_slack_alert,
}

with DAG(
    'sportmetrics_pipeline_yoan_mboulou',
    default_args=default_args,
    schedule_interval='0 6 * * *',
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=['sportmetrics', 'production'],
) as dag:

    execute_n8n_workflow = PythonOperator(
        task_id="execute_n8n_workflow",
        python_callable=execute_n8n_workflow
    )

    dbt_run = DockerOperator(
        task_id="dbt_run",
        image="ghcr.io/dbt-labs/dbt-bigquery:1.8.0",
        command="run --profiles-dir /usr/app --project-dir /usr/app",
        working_dir="/usr/app",
        mounts=[
        Mount(
            source=os.environ['SPORTS_METRICS_PATH'],
            target="/usr/app",
            type="bind"
        ),
        Mount(
            source=f"{os.environ['SPORTS_METRICS_PATH']}/credentials/gcp_keyfile.json",
            target="/root/.google/credentials/gcp_keyfile.json",
            type="bind"
        ),
        ],
        environment={
            "GOOGLE_APPLICATION_CREDENTIALS": "/root/.google/credentials/gcp_keyfile.json"
        },
        docker_url="tcp://host.docker.internal:2375",
        auto_remove="success",
        network_mode="bridge",
    )

    dbt_snapshot = DockerOperator(
        task_id="dbt_snapshot",
        image="ghcr.io/dbt-labs/dbt-bigquery:1.8.0",
        command="snapshot --profiles-dir /usr/app --project-dir /usr/app",
        working_dir="/usr/app",
        mounts=[
            Mount(source=os.environ['SPORTS_METRICS_PATH'], target="/usr/app", type="bind"),
            Mount(source=f"{os.environ['SPORTS_METRICS_PATH']}/credentials/gcp_keyfile.json", target="/root/.google/credentials/gcp_keyfile.json", type="bind"),
        ],
        environment={"GOOGLE_APPLICATION_CREDENTIALS": "/root/.google/credentials/gcp_keyfile.json"},
        docker_url="tcp://host.docker.internal:2375",
        auto_remove="success",
        network_mode="bridge",
    )

    ml_injury = DockerOperator(
        task_id="ml_injury_prevention",
        image="python:3.11-slim",
        command="bash -c 'pip install google-cloud-bigquery pandas xgboost scikit-learn db-dtypes -q && python prevention_blessure.py'",
        working_dir="/app",
        mounts=[
            Mount(
                source=os.environ['SPORTS_METRICS_PATH'] + "/ml",
                target="/app",
                type="bind"
            ),
            Mount(
                source=os.environ['SPORTS_METRICS_PATH'] + "/credentials/gcp_keyfile.json",
                target="/root/.google/credentials/gcp_keyfile.json",
                type="bind"
            ),
        ],
        environment={
            "GOOGLE_APPLICATION_CREDENTIALS": "/root/.google/credentials/gcp_keyfile.json",
            "GCP_PROJECT_ID": os.environ['GCP_PROJECT_ID'],
            "GCP_DATASET_ID": os.environ['GCP_DATASET_ID'],
        },
        docker_url="tcp://host.docker.internal:2375",
        auto_remove="success",
        network_mode="bridge",
    )

    ml_clustering = DockerOperator(
        task_id="ml_player_clustering",
        image="python:3.11-slim",
        command="bash -c 'pip install google-cloud-bigquery pandas scikit-learn db-dtypes -q && python prevision_equipe.py'",
        working_dir="/app",
        mounts=[
            Mount(
                source=os.environ['SPORTS_METRICS_PATH'] + "/ml",
                target="/app",
                type="bind"
            ),
            Mount(
                source=os.environ['SPORTS_METRICS_PATH'] + "/credentials/gcp_keyfile.json",
                target="/root/.google/credentials/gcp_keyfile.json",
                type="bind"
            ),
        ],
        environment={
            "GOOGLE_APPLICATION_CREDENTIALS": "/root/.google/credentials/gcp_keyfile.json",
            "GCP_PROJECT_ID": os.environ['GCP_PROJECT_ID'],
            "GCP_DATASET_ID": os.environ['GCP_DATASET_ID'],
        },
        docker_url="tcp://host.docker.internal:2375",
        auto_remove="success",
        network_mode="bridge",
    )


    execute_n8n_workflow >> dbt_run >> dbt_snapshot >> [ml_injury, ml_clustering]