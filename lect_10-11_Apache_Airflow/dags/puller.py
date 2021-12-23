from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context

with DAG(
        'puller',
        schedule_interval="@once",
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['FreeUni'],
) as dag:
    def message(dag_run):

        print("***************", dag_run.conf)

        print(f"Remotely received value of {dag_run.conf['message']} for key=message")


    value_1 = [1, 2, 3]

    def pullk(**kwargs):
        ti = kwargs['ti']
        extract_data_string = ti.xcom_pull(dag_id='pusher',task_ids='push', key='value from pusher 1',include_prior_dates=True)
        # order_data = json.loads(extract_data_string)
        print("********************",extract_data_string)

    def pullk3(**kwargs):
        extract_data_string = kwargs['ti'].xcom_pull(dag_id='pusher',task_ids='push3',key='order_data',include_prior_dates=True)
        # order_data = json.loads(extract_data_string)

        print("***************************",extract_data_string)

    message = PythonOperator(
        task_id="message",
        python_callable=message)

    pull = PythonOperator(
        task_id='pull',
        python_callable=pullk,
    )


    pull3 = PythonOperator(
        task_id='pull3',
        python_callable=pullk3,
    )
    message>>pull >> pull3
