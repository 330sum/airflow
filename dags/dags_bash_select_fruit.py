from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
        dag_id="dags_bash_select_fruit",
        schedule="10 0 * * 6#1",
        start_date=pendulum.datetime(2024, 2, 1, tz="Asia/Seoul"),
        catchup=False
) as dag:
    t1_orange = BashOperator(
        task_id="t1_orange",
        bash_command="/opt/airflow/plugins/shell/select_fruit.sh ORANGE",
        # task를 실행하는 주체는 워커컨테이너임. 워커 컨테이너가 알 수 있도록 이렇게 작성
        # chmod +x select_fruits.sh
        # ./select_fruits.sh ORANGE
    )

    t2_avocado = BashOperator(
        task_id="t2_avocado",
        bash_command="/opt/airflow/plugins/shell/select_fruit.sh AVOCADO",
    )

    t1_orange >> t2_avocado
