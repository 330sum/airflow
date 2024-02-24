from airflow.models.dag import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_operator", # dag과 python파일명을 일치 시키는 것이 관례
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    # dagrun_timeout=datetime.timedelta(minutes=60), # 60분 이상 dag이 돌면 실패되도록 설정
    # tags=["example", "example2"],
    # params={"example_key": "example_value"},
) as dag:
    bash_task1 = BashOperator( # Operator를 통해 만들어 지는 것이 task 객체
        task_id="bash_task1", # task객체명과 task_id 일치 시키기
        bash_command="echo whoami",
    )
    
    bash_task2 = BashOperator(
        task_id="bash_task2",
        bash_command="echo $HOSTNAME",
    )
    
    bash_task1 >> bash_task2