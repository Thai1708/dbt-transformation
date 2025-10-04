from datetime import datetime, timedelta
from airflow import DAG
from docker.types import Mount

from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

from airflow.providers.docker.operators.docker import DockerOperator
import subprocess

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}


def run_elt_script():
    script_path = "/opt/airflow/elt_script/elt_script.py"
    result = subprocess.run(["python", script_path],
                            capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Script failed with error: {result.stderr}")
    else:
        print(result.stdout)


dag = DAG(
    'film_classification_dag',
    default_args=default_args,
    description='An ELT workflow with dbt',
    schedule_interval="@daily",
    start_date=datetime(2025, 9, 30),
    catchup=False,
    is_paused_upon_creation=False,
)

t1 = ExternalTaskSensor(
        task_id="wait_init_source_target_dag",
        external_dag_id="init_source_target_dag",       # DAG cần chờ
        external_task_id='run_elt_script',         # None = chờ cả DAG
        poke_interval=30,              # check mỗi 30s
        mode="reschedule"                    # hoặc "reschedule"
)

t2 = DockerOperator(
    task_id='dbt_run',
    image='ghcr.io/dbt-labs/dbt-postgres:1.4.7',
    command=[
        "run",
        "--profiles-dir",
        "/root",
        "--project-dir",
        "/dbt",
        "--full-refresh"
    ],
    auto_remove=True,
    docker_url="unix:///var/run/docker.sock",
    network_mode="custom-elt-project_elt_network",
    mounts=[
        Mount(source='/mnt/d/PHAMVANTHAI/custom-elt-project/postgres_transformations',
              target='/dbt', type='bind'),
        Mount(source='/home/minhthai/.dbt', target='/root', type='bind'),
    ],
    dag=dag
)

t1 >> t2