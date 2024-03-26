from airflow.decorators import dag, task
from datetime import datetime, timedelta
from pendulum.tz.timezone import Timezone

kst = Timezone("Asia/Seoul")
date_time = datetime.now(tz=kst)
today = date_time.strftime('%Y-%m-%d')
start_date = date_time - timedelta(days=1)


@dag(
    dag_id='python_args_test',
    start_date=start_date,
    schedule=None,
    tags=['args']
)
@task
def test(name, age):
    print('type =====================> ', type(name))
    print('name =====================> ', name)
    print('type =====================> ', type(age))
    print('age =====================> ', age)


task_1 = test('sumin', 29)

task_1