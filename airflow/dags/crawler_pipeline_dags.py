from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import time

PROJECT_DIR = "/opt/project"


def gen_timestamp(**context):
    ts = str(int(time.time()))
    context["ti"].xcom_push(key="ts", value=ts)


with DAG(
    dag_id="crawler_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    get_ts = PythonOperator(
        task_id="gen_timestamp",
        python_callable=gen_timestamp,
    )

    crawl = BashOperator(
        task_id="crawl_main",
        bash_command=(
            "python "
            f"{PROJECT_DIR}/crawl/crawler/main.py "
            "--timestamp {{ ti.xcom_pull(task_ids='gen_timestamp', key='ts') }}"
        ),
    )

    push_es_issue = BashOperator(
        task_id="push_es_issue",
        bash_command=(
            "python "
            f"{PROJECT_DIR}/pipeline/es_push.py "
            "--root crawl/{{ ti.xcom_pull(task_ids='gen_timestamp', key='ts') }} "
            "--index issues"
        ),
    )

    push_es_full_report = BashOperator(
        task_id="push_es_full_report",
        bash_command=(
            "python "
            f"{PROJECT_DIR}/pipeline/es_push_full_report.py "
            "--root issues_output/{{ ti.xcom_pull(task_ids='gen_timestamp', key='ts') }} "
            "--index issue_full_report"
        ),
    )

    push_minio = BashOperator(
        task_id="push_minio",
        bash_command=(
            "python "
            f"{PROJECT_DIR}/pipeline/minio_push.py "
            "repos_code/{{ ti.xcom_pull(task_ids='gen_timestamp', key='ts') }} "
            "--bucket repos "
            "--prefix {{ ti.xcom_pull(task_ids='gen_timestamp', key='ts') }}"
        ),
    )

    get_ts >> crawl >> [push_es_issue, push_es_full_report, push_minio]
