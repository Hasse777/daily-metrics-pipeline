from pathlib import Path
from datetime import datetime

from airflow.decorators import dag
from airflow.providers.vertica.operators.vertica import VerticaOperator
from airflow.sensors.external_task import ExternalTaskSensor


@dag(
    dag_id='load_data_mart_DAG',
    start_date=datetime(2022, 10, 1),
    schedule='@daily',
    max_active_runs=1,
    catchup=True
)
def load_data_mart_DAG():
    # Здесь можете подставить свою схему
    my_DWH_schema = 'VT251030DBB337__DWH'

    VERTICA_CON = 'VerticaConn'

    # Ждем завершения дага загрузки stg
    wait_dds = ExternalTaskSensor(
        task_id = 'wait_dds_dag',
        external_dag_id='load_data_to_dds_DAG',
        external_task_id=None,
        allowed_states=['success'],
        failed_states=['failed'],
        # Я конечно не уверен, то что это хорошее решение. Получается у нас каждые 300 секунд будет опрос
        poke_interval=300, 
        mode='reschedule'
    )

    task_create_all_tables_dds = VerticaOperator(
        task_id='load_data_mart',
        vertica_conn_id=VERTICA_CON,
        sql='sql/07_mart.sql',
        params={'schema_dwh': my_DWH_schema}
    )

    wait_dds >> task_create_all_tables_dds

dag = load_data_mart_DAG()