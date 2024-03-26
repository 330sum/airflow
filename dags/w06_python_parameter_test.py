from airflow import DAG
from datetime import datetime, timedelta
from pendulum.tz.timezone import Timezone
from airflow.operators.python import PythonOperator

kst = Timezone("Asia/Seoul")
date_time = datetime.now(tz=kst)
today = date_time.strftime('%Y-%m-%d')
start_date = date_time - timedelta(days=1)

with DAG(
        dag_id='python_parameter_test',
        start_date=start_date,
        schedule=None,
        tags=['parameter']
) as dag:
    def python_func(name, age, *args, **kwargs):
        print('type =====================> ', type(name))
        print('name =====================> ', name)
        print('type =====================> ', type(age))
        print('age =====================> ', age)
        print('type =====================> ', type(args))
        print(args)
        print(args[0])
        # print(args[2] if len(args[2])>=2 else None)
        print('type =====================> ', type(kwargs))
        print(kwargs)
        for k in kwargs:
            print(k)
        task_instance = kwargs['task_instance']
        print(task_instance, '========task instance')
        print(kwargs.get('사는곳'))
        print(kwargs.get('없는파라미터'), '=============================')
        print(kwargs['고향'])
        # print(kwargs['없는파라미터'])


    python_parameter_test_task_01 = PythonOperator(
        task_id='python_parameter_test_task_01',
        python_callable=python_func,
        op_args=['수민', 29, '한국', '여자'],
        op_kwargs={'사는곳': '서울', '고향': '사천'}
    )
