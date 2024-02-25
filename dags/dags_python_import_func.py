from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from common.common_func import get_sftp # plugins 쓰면 airflow 에서 에러 남 (.env 파일 참고)

with DAG(
        dag_id="dags_python_import_func",
        schedule="30 6 * * *",
        start_date=pendulum.datetime(2024, 2, 1, tz="Asia/Seoul"),
        catchup=False
) as dag:
    task_get_sftp = PythonOperator(
        task_id='task_get_sftp',
        python_callable=get_sftp # 괄호 쓰면 안됨
    )
