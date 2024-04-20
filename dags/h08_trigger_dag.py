from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from pendulum.tz.timezone import Timezone

kst = Timezone("Asia/Seoul")

date_now = datetime.now(tz=kst)
start_date = date_now - timedelta(days=1)


@dag(
    dag_id='h08_trigger_dag',
    schedule=None,
    start_date=start_date,
    description='dag들 부르기',
    catchup=False,
    tags=['test'],
)
def full_main():
    with TaskGroup(group_id='data_01') as data_01:
        call_data_01_dag = TriggerDagRunOperator(
            task_id='call_data_01_dag',
            trigger_dag_id='data_01_dag',  # dag명
            wait_for_completion=True
        )
        call_data_01_dag  # group안에 있는 task들 실행 순서

    with TaskGroup(group_id='data_02') as data_02:
        with TaskGroup(group_id='data_02_s') as data_02_s:
            call_data_02_s1_dag = TriggerDagRunOperator(
                task_id='call_data_02_s1_dag',
                trigger_dag_id='data_02_s1_dag',
                wait_for_completion=True
            )
            call_data_02_s2_dag = TriggerDagRunOperator(
                task_id='call_data_02_s2_dag',
                trigger_dag_id='data_02_s2_dag',
                wait_for_completion=True
            )
            call_data_02_s1_dag >> call_data_02_s2_dag  # group안에 있는 task들 실행 순서

        data_02_s  # group안에 있는 group들 실행 순서

    with TaskGroup(group_id='data_03') as data_03:
        call_data_03_dag = TriggerDagRunOperator(
            task_id='call_data_03_dag',
            trigger_dag_id='data_03_dag',
            wait_for_completion=True
        )
        call_data_03_dag  # group안에 있는 task들 실행 순서

    data_01 >> data_02 >> data_03  # group_id로 실행


full_main()
