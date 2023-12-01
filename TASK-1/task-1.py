from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

with DAG(
    'Task-1-Taufik',
    description='Task-1',
    schedule_interval='*/5 * * * *',
    start_date=datetime(2023, 11, 20),
    catchup=False,
) as dag:
    def push_var_to_xcom(ti=None):
        ti.xcom_push(key='comic_title', value='One Piece')
        ti.xcom_push(key='author', value='Echiro Oda')

    def pull_multiple(**kwargs):
        ti = kwargs['ti']
        komik = ti.xcom_pull(task_ids='Task-1-Taufik', key='comic_title')
        penulis = ti.xcom_pull(task_ids='Task-1-Taufik', key='author')

        print(f'Judulnya: {komik}')
        print(f'Penulisnya: {penulis}')

    push_var = PythonOperator(
        task_id='Task-1-Taufik',
        python_callable=push_var_to_xcom,
        provide_context=True
    )
    pull_multiple = PythonOperator(
        task_id='hasil_pull',
        python_callable=pull_multiple,
        provide_context=True
    )
    
    push_var >> pull_multiple
