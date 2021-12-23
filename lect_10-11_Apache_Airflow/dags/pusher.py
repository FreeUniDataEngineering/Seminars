import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from operators.meta_operator import MetaOperator
with DAG(
        'pusher',
        schedule_interval="@once",
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['FreeUni'],
) as dag:


    def push(**kwargs):
        value_1 = [1, 2, 3]
        kwargs['ti'].xcom_push(key='value from pusher 1', value=value_1)


    def push3(**kwargs):
        """Pushes an XCom without a specific target"""
        ti = kwargs['ti']
        value_2 = [2, 3, 4, 5, 6]
        ti.xcom_push('order_data', value_2)


    push1 = PythonOperator(
        task_id='push',
        python_callable=push,
    )
    push3 = PythonOperator(
        task_id='push3',
        python_callable=push3,
        # conf={"message2": "Hello World2"},
    )
    trigger = TriggerDagRunOperator(
        task_id="trigger_dagrun",
        trigger_dag_id="puller",
        conf={"message": "Hello Puller"}
    )

    meta = MetaOperator(
        task_id='meta',
        operator_type='PythonOperator'
    )
    push1 >> push3 >> trigger>>meta
