from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum.tz.timezone import Timezone

kst = Timezone("Asia/Seoul")  # 한국시간 설정

date_now = datetime.now(tz=kst)  # 한국의 현재 시간
today = date_now.strftime('%Y-%m-%d')  # 한국의 현재 날짜

start_date = date_now - timedelta(days=1)


@dag(
    dag_id='trigger_dag_a',
    description='트리거 연습용 데그 a',
    schedule=None,
    start_date=start_date,
    schedule_interval='',
    catchup=False,
    tags=['trigger']
)
def main():
    @task(task_id="dag_a_task_1")
    def task_a1():
        print("테스크_a1")
        print("kst ===========================>", kst)
        print("date_now ===========================>", date_now)
        print("today ==================>", today)
        print("start_date ===========================>", start_date)

    @task(task_id="dag_a_task_2")
    def task_a2():
        print("테스크_a2")

    @task(task_id="dag_a_task_3")
    def task_a3():
        print("테스크_a3")

    tigger_task = TriggerDagRunOperator(
        task_id='tigger_task',
        trigger_dag_id='trigger_dag_b',
        wait_for_completion=False,
        conf={
            "a1": "aaaaaaaaa",
            "b1": "bbbbbbbbbb"
        }

    )

    task_a1 = task_a1()
    task_a2 = task_a2()
    task_a3 = task_a3()

    task_a1 >> task_a2 >> task_a3 >> tigger_task


main()
