import pathlib
from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.time_delta_sensor import TimeDeltaSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor


def get_sql_string(_dags, _path):
    """
    To read the SQL files and return the SQL query as a string

    :param _dags: The base root folder
    :type _dags: str
    :param _path: Location of the SQL file
    :type _path: str
    :return: The SQL query
    :rtype: str
    """

    with open(f"{_dags}/{_path}", 'r') as sql_file:
        sql = sql_file.read()
        sql_file.close()

    return sql


all_jobs = list()

dags = str(pathlib.Path(__file__).parent.absolute())

default_args = {
    'owner': 'data-think-tank',
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 12, 31, 17)
}

dag = DAG(
    dag_id='L3_lgd_store',
    schedule_interval='0 */3 * * *',
    default_args=default_args,
    catchup=False
)

external_task = ExternalTaskSensor(
    task_id=f'wait_L2_visibility_lgd_store',
    dag=dag,
    external_dag_id='L2_visibility_lgd_store',
    external_task_id='move_L1_to_L2'
)

#  FACT_lgd_store
fact_store = BigQueryExecuteQueryOperator(
    task_id='fact_store',
    dag=dag,
    sql=get_sql_string(dags, 'source/sql/dwh/L3/lgd/fact_store.sql'),
    destination_dataset_table='logee-data-prod.L3_lgd.fact_store',
    write_disposition='WRITE_APPEND',
    allow_large_results=True,
    use_legacy_sql=False,
    location='asia-southeast2',
    schema_update_options=[
        "ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"
    ],
    time_partitioning={
        "type": "DAY",
        "field": "modified_at"
    },
    labels={
        "type": "scheduled",
        "level": "landing",
        "runner": "airflow"
    }
)

external_task >> fact_store

#  FACT_lgd_store_list_address
fact_store_list_address = BigQueryExecuteQueryOperator(
    task_id='fact_store_list_address',
    dag=dag,
    sql=get_sql_string(dags, 'source/sql/dwh/L3/lgd/fact_store_list_address.sql'),
    destination_dataset_table='logee-data-prod.L3_lgd.fact_store_list_address',
    write_disposition='WRITE_APPEND',
    allow_large_results=True,
    use_legacy_sql=False,
    location='asia-southeast2',
    schema_update_options=[
        "ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"
    ],
    time_partitioning={
        "type": "DAY",
        "field": "modified_at"
    },
    labels={
        "type": "scheduled",
        "level": "landing",
        "runner": "airflow"
    }
)

external_task >> fact_store_list_address
