
import airflow

from airflow import DAG

from airflow.operators.python_operator import PythonOperator

from airflow.operators.dummy_operator import DummyOperator

from datetime import timedelta

import logging

args = {

    'owner': 'airflow',

    'start_date': airflow.utils.dates.days_ago(1)

}

dag = DAG(

    dag_id="test_dag20_jinja_xcom_pythonoperator",

    default_args=args,

    schedule_interval=timedelta(days=1),
    tags=["FreeUni"]

)

def simple_return(**context):

    write2file("\n**************************************************************************************\n")

    write2file("**************************************************************************************\n")

    write2file("ds = " + str(context['ds']) + "\n")

    write2file("run_id = " + str(context['run_id']) + "\n")

    write2file("task.owner = " + str(context['task'].owner) + "\n")

    write2file("task.task_id = " + str(context['task'].task_id) + "\n")

    write2file("ti.task_id = " + str(context['ti'].task_id) + "\n")

    write2file("ti.dag_id = " + str(context['ti'].dag_id) + "\n")

    write2file("ti.start_date = " + str(context['ti'].start_date) + "\n")

    write2file("ti.end_date = " + str(context['ti'].end_date) + "\n")

    write2file("ti.duration = " + str(context['ti'].duration) + "\n")

    write2file("ti.state = " + str(context['ti'].state) + "\n")

    write2file("ti._try_number = " + str(context['ti']._try_number) + "\n")

    write2file("ti.max_tries = " + str(context['ti'].max_tries) + "\n")

    write2file("ti.hostname = " + str(context['ti'].hostname) + "\n")

    write2file("ti.unixname = " + str(context['ti'].unixname) + "\n")

    write2file("ti.job_id = " + str(context['ti'].job_id) + "\n")

    write2file("ti.pool = " + str(context['ti'].pool) + "\n")

    write2file("ti.queue = " + str(context['ti'].queue) + "\n")

    write2file("ti.priority_weight = " + str(context['ti'].priority_weight) + "\n")

    write2file("ti.operator = " + str(context['ti'].operator) + "\n")

    write2file("ti.queued_dttm = " + str(context['ti'].queued_dttm) + "\n")

    write2file("ti.pid = " + str(context['ti'].pid) + "\n")

    write2file("dag_run.id = " + str(context['dag_run'].id) + "\n")

    write2file("dag_run.dag_id = " + str(context['dag_run'].dag_id) + "\n")

    write2file("dag_run.execution_date = " + str(context['dag_run'].execution_date) + "\n")

    write2file("dag_run.start_date = " + str(context['dag_run'].start_date) + "\n")

    write2file("dag_run.end_date = " + str(context['dag_run'].end_date) + "\n")

    write2file("dag_run._state = " + str(context['dag_run']._state) + "\n")

    write2file("dag_run.run_id = " + str(context['dag_run'].run_id) + "\n")

    write2file("dag_run.external_trigger = " + str(context['dag_run'].external_trigger) + "\n")

    write2file("test_mode = " + str(context['test_mode']) + "\n")

    write2file("t_dummy.start_date = " + str(t_dummy.start_date))

    return {"start_date": "2019-09-10", "end_date": "2019-09-15"}

def callable_second(**context):

    dates_dict = context['task_instance'].xcom_pull(task_ids='t1_simple_return')

    start_date_str = context['task_instance'].xcom_pull(task_ids='t1_simple_return')['start_date']

    ti_simple_return = context['dag_run'].get_task_instance("t1_simple_return")

    write2file("=====> simple_return.start_date = " + str(ti_simple_return.start_date) + ", simple_return.end_date = " + str(ti_simple_return.end_date))

    write2file("start_date = " + start_date_str)

def write2file(str2log):

    with open("/opt/airflow/dags/test_dag20_pythonoperator.txt", 'a') as log:

        log.write(str2log)

t_dummy = DummyOperator(

    task_id="dummy_op",

    dag=dag

)

t1_simple_return = PythonOperator(

    task_id="t1_simple_return",

    provide_context=True,

    python_callable=simple_return,

    dag=dag

)

t2_second_python = PythonOperator(

    task_id="t2_analyze_dates",

    provide_context=True,

    python_callable=callable_second,

    dag=dag

)

t_dummy >> t1_simple_return >> t2_second_python