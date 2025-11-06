from pathlib import Path
from datetime import datetime

from airflow.decorators import dag
from airflow.providers.vertica.operators.vertica import VerticaOperator
from airflow.sensors.external_task import ExternalTaskSensor


@dag(
    dag_id='load_data_to_dds_DAG',
    start_date=datetime(2022, 10, 1),
    schedule='@daily',
    max_active_runs=1,
    catchup=True
)
def load_data_to_dds_DAG():
    # Текущая директория где выполняется скрипт
    current_dir = Path(__file__).parent

    # Здесь можете подставить свою схему
    my_STG_schema = 'VT251030DBB337__STAGING'
    my_DWH_schema = 'VT251030DBB337__DWH'

    VERTICA_CON = 'VerticaConn'

    # Ждем завершения дага загрузки stg
    wait_stg = ExternalTaskSensor(
        task_id = 'wait_stg_dag',
        external_dag_id='load_data_to_stg_DAG',
        external_task_id=None,
        allowed_states=['success'],
        failed_states=['failed'],
        # Я конечно не уверен, то что это хорошее решение. Получается у нас каждые 300 секунд будет опрос
        poke_interval=300,
        mode='reschedule'
    )

    task_create_all_tables_dds = VerticaOperator(
        task_id='create_all_tables_dds',
        vertica_conn_id=VERTICA_CON,
        sql='sql/05_create_dds_tables.sql',
        params={'schema_dwh': my_DWH_schema}
    )

    task_load_data_to_dds = VerticaOperator(
        task_id='load_data_to_dds',
        vertica_conn_id=VERTICA_CON,
        sql='sql/06_load_data_to_dds.sql',
        params={
            'schema_dwh': my_DWH_schema,
            'schema_stg': my_STG_schema
        }
    )

    wait_stg >> task_create_all_tables_dds >> task_load_data_to_dds

dag = load_data_to_dds_DAG()