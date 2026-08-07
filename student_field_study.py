import pendulum
import pandas as pd
from datetime import timedelta

from airflow import DAG
from airflow.models import XCom
from airflow.utils.db import provide_session
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

doc_md="""
### student_field_study

### PURPOSE
create dw student_field_study

"""


def extract_load():
    today = pendulum.now(tz = "Asia/Jakarta").strftime("%Y-%m-%d %H:%M:%S")
    print(f"hari ini : {today}")

    src = PostgresHook(postgres_conn_id='local_post')
    dest = PostgresHook(postgres_conn_id='local_post')
    dest_engine = dest.get_sqlalchemy_engine()
    src_conn = src.get_conn()

    query = """SELECT * FROM datalake.student_field_study;"""

    print("extracting data")

    datas = pd.read_sql_query(query, src_conn)

    print("data extracted")

    datas["data_updated_at"] = today
    datas["data_updated_at"] = pd.to_datetime(datas['data_updated_at'])

    print(f"jumlah row : {datas.shape[0]}")

    datas.to_sql(
        "l1_student_field_study",
        dest_engine,
        if_exists="replace",
        index=False,
        schema="datawarehouse",
        chunksize=1000
    )

@provide_session
def cleanup_xcom(session=None, **context):
    dag=context['dag']
    dag_id = dag._dag_id
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()

default_args = {
    "owner" : "ria",
    "retries" : 3,
    "retry_delay" : timedelta(seconds=3)
}  

with DAG(
    dag_id = "dw_student_field_study",
    default_args = default_args,
    start_date = pendulum.datetime(2022, 9, 19, tz="Asia/Jakarta"),
    schedule_interval = "@once",
    doc_md = doc_md,
    catchup = False,
    tags = ["datawarehouse"] 
) as dag:

    start_task = DummyOperator(
        task_id = "start"
    )

    extract_load_ = PythonOperator(
        task_id = "extract_load_datas",
        python_callable = extract_load
    )

    delete_xcom = PythonOperator(
        task_id='delete_xcom',
        python_callable = cleanup_xcom,
        provide_context = True
    )    

    end_task = DummyOperator(
        task_id = "end"
    )

    start_task >> extract_load_ >> delete_xcom >> end_task