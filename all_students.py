import pendulum
from datetime import timedelta
import pandas as pd
import re
import sqlparse

from airflow import DAG
from airflow.models import XCom
from airflow.utils.db import provide_session
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

doc_md = """
### all students data

### PURPOSE
create dm all students data

"""

def extract_table_names(query):
    # Mengurai query SQL untuk menemukan nama tabel
    parsed = sqlparse.parse(query)
    table_names = set()

    for statement in parsed:
        for token in statement.tokens:
            if token.ttype and 'TABLE' in token.value.upper():
                # Mengambil nama tabel dari statement SQL
                table_name = token.get_real_name()
                table_names.add(table_name)

    return table_names

def check_table_updates():
    today = pendulum.now(tz="Asia/Jakarta").strftime("%Y-%m-%d")
    
    src = PostgresHook(postgres_conn_id='local_post')
    query = """
    with biodata as(
        select
            sb.student_id,
            sd.student_name as name,
            sfs.field_of_study,
            ss.specialization,
            sd.year_of_admission,
            sd.expected_year_of_graduation
        from datawarehouse.l1_student_biodata sb
        left join datawarehouse.l1_student_details sd on sb.student_id = sd.student_id
        left join datawarehouse.l1_student_field_study sfs on sd.student_id = sfs.student_id
        left join datawarehouse.l1_student_specialization ss on sfs.student_id = ss.student_id
    ),
    total_biaya as(
        select
            student_id,
            sum(fees - discount_on_fees) as actual_fees
        from datawarehouse.l1_student_tuition_fees
        group by 1
    )
    select
        b.*,
        tb.actual_fees
    from biodata b
    left join total_biaya tb on b.student_id = tb.student_id;
    """
    
    print("Checking table updates...")
    table_names = extract_table_names(query)
    updated_tables = []

    for table_name in table_names:
        table_name = table_name.strip().strip('"')  # Membersihkan nama tabel
        query = f"SELECT MAX(data_updated_at) FROM datawarehouse.{table_name};"
        last_updated_date = src.get_first(query)[0]

        if last_updated_date != today:
            raise Exception(f"Table {table_name} has not been updated for today ({today}).")
        else:
            updated_tables.append(table_name)  # Tambahkan tabel yang diperbarui ke daftar

    if updated_tables:
        print(f"Tables successfully checked and updated: {', '.join(updated_tables)}")
    else:
        print("All tables have been updated.")

def extract_load() :
    check_table_updates()  # Memeriksa pembaruan tabel
    today = pendulum.now(tz="Asia/Jakarta").strftime("%Y-%m-%d %H:%M:%S")

    print("Connecting to source and destinations")
    src = PostgresHook(postgres_conn_id='local_post')
    dest = PostgresHook(postgres_conn_id='local_post')
    dest_engine = dest.get_sqlalchemy_engine()
    src_conn = src.get_conn()
    
    query = f"""
    with biodata as(
        select
            sb.student_id,
            sd.student_name as name,
            sfs.field_of_study,
            ss.specialization,
            sd.year_of_admission,
            sd.expected_year_of_graduation
        from datawarehouse.l1_student_biodata sb
        left join datawarehouse.l1_student_details sd on sb.student_id = sd.student_id
        left join datawarehouse.l1_student_field_study sfs on sd.student_id = sfs.student_id
        left join datawarehouse.l1_student_specialization ss on sfs.student_id = ss.student_id
    ),
    total_biaya as(
        select
            student_id,
            sum(fees - discount_on_fees) as actual_fees
        from datawarehouse.l1_student_tuition_fees
        group by 1
    )
    select
        b.*,
        tb.actual_fees
    from biodata b
    left join total_biaya tb on b.student_id = tb.student_id;
    """

    print("extracting data")
    datas = pd.read_sql_query(query, src_conn)
    print("data extracted")
    print(f"jumlah row : {datas.shape[0]}")

    datas["data_updated_at"] = today

    print("data loaded to local_post")

    datas.to_sql(
        "l1_all_students",
        dest_engine,
        if_exists="replace",
        index=False,
        schema="datamart",
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
    dag_id = "all_students",
    default_args = default_args,
    start_date = pendulum.datetime(2022, 9, 19, tz="Asia/Jakarta"),
    schedule_interval = "@once",
    doc_md = doc_md,
    catchup = False,
    tags = ["datamart"] 
) as dag:

    start_task = DummyOperator(
        task_id = "start"
    )

    check_table = PythonOperator(
        task_id = "check_table_updates",
        python_callable = check_table_updates
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

    start_task >> check_table >> extract_load_ >> delete_xcom >> end_task
