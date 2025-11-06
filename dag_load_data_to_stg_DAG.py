from pathlib import Path
from datetime import datetime

from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.vertica.operators.vertica import VerticaOperator

from helpers import ConnectionBuilder
from helpers import load_data_csv_from_ps



@dag(
    dag_id='load_data_to_stg_DAG',
    start_date=datetime(2022, 10, 1),
    schedule='@daily',
    #schedule='00 00 * * 1',
    end_date=datetime(2022, 10, 10),
    max_active_runs=1,
    catchup=True
)
def load_data_to_stg_DAG():
    # Текущая директория где выполняется скрипт
    current_dir = Path(__file__).parent
    # Путь куда мы сохраняем csv файлы из postgr
    data_dir_csv = current_dir / 'data'

    # Здесь можете подставить свою схему
    my_STG_schema = 'VT251030DBB337__STAGING'
    me_DWH_schema = 'VT251030DBB337__DWH'

    # pg_conn Вспомогательный класс для соединений с postrg
    pg_conn = ConnectionBuilder.pg_conn('PostgreConn')
    VERTICA_CON = 'VerticaConn'

    # Создаем stg таблицы в vertica
    task_create_stg_tables = VerticaOperator(
        task_id='create_stg_tables',
        vertica_conn_id=VERTICA_CON,
        sql='sql/01_create_all_tables_stg.sql',
        params={'schema_stg': my_STG_schema}
    )

    # Создаем витрину в vertica
    task_create_mart_tables = VerticaOperator(
        task_id='create_mart_tables',
        vertica_conn_id=VERTICA_CON,
        sql='sql/02_create_tables_marts.sql',
        params={'schema_dwh': me_DWH_schema}
    )

    # Выгружаем данные из таблицы transactions в csv файл
    task_load_transactions_to_csv = PythonOperator(
        task_id='load_transactions_to_csv',
        python_callable=load_data_csv_from_ps,
        op_kwargs={
            'pg_conn': pg_conn,
            'ds': '{{ ds }}',
            'output_dir': data_dir_csv,
            'table_name': 'transactions',
            'date_column': 'transaction_dt'
        }
    )

    # Выгружаем данные из таблицы currencies в csv файл
    task_load_currencies_to_csv = PythonOperator(
        task_id='load_currencies_to_csv',
        python_callable=load_data_csv_from_ps,
        op_kwargs={
            'pg_conn': pg_conn,
            'ds': '{{ ds }}',
            'output_dir': data_dir_csv,
            'table_name': 'currencies',
            'date_column': 'date_update'
        }
    )
    
    # Выгружаем данные из csv в stg таблицу transactions
    task_load_transactions_to_psgr = VerticaOperator(
        task_id = 'load_transactions_to_psgr',
        vertica_conn_id=VERTICA_CON,
        sql='sql/03_load_transactions_to_vertica.sql',
        params={
            'schema_stg': my_STG_schema,
            'path': str(data_dir_csv)
            }
    )

    # Выгружаем данные из csv в stg таблицу currencies
    task_load_currencies_to_psgr = VerticaOperator(
        task_id = 'load_currencies_to_psgr',
        vertica_conn_id=VERTICA_CON,
        sql='sql/04_load_currencies_to_vertica.sql',
        params={
            'schema_stg': my_STG_schema,
            'path': str(data_dir_csv)
            }
    )
    preparation_tables_data = [task_create_stg_tables, task_create_mart_tables, task_load_transactions_to_csv, task_load_currencies_to_csv]

    preparation_tables_data >> task_load_transactions_to_psgr >> task_load_currencies_to_psgr

dag = load_data_to_stg_DAG()
