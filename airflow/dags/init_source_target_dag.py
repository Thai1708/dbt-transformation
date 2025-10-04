from datetime import datetime, timedelta
from airflow import DAG
from docker.types import Mount

from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable


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
    'init_source_target_dag',
    default_args=default_args,
    description='An ELT workflow with dbt',
    schedule_interval="@daily",
    start_date=datetime(2025, 9, 30),
    catchup=False,
    is_paused_upon_creation=False,
)

t1 = PythonOperator(
    task_id='run_elt_script',
    python_callable=run_elt_script,
    dag=dag,
)

send_email = EmailOperator(
    task_id='send_success_email',
    to=Variable.get("my_email"),
    subject='[Airflow] DAG {{ dag.dag_id }} chạy thành công',
    html_content="""
        <h2 style="color:red;">Xin chào từ Airflow!</h2>
        <p>DAG <b>{{ dag.dag_id }}</b> đã chạy thành công lúc {{ ds }}.</p>
        <p><i>Đây là email với nội dung HTML.</i></p>
        """,
    dag=dag,
)

t1 >> send_email