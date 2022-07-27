import pathlib
from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.time_delta_sensor import TimeDeltaSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
# from airflow.sensors.external_task import ExternalTaskSensor


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
    dag_id='L3_lgd_companies',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False
)

wait = TimeDeltaSensor(
    task_id='wait_for_data',
    dag=dag,
    delta=timedelta(hours=9)
)

#  FACT_LGD_COMPANIES
fact_visit = BigQueryExecuteQueryOperator(
    task_id='fact_lgd_companies',
    dag=dag,
    sql=get_sql_string(dags, 'source/sql/dwh/L3/visibility/lgd_companies.sql'),
    destination_dataset_table='logee-data-prod.L3_visibility.fact_lgd_companies${{ ds_nodash }}',
    write_disposition='WRITE_APPEND',
    allow_large_results=True,
    use_legacy_sql=False,
    location='asia-southeast2',
    schema_update_options=[
        "ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"
    ],
    time_partitioning={
        "type": "DAY"
    },
    labels={
        "type": "scheduled",
        "level": "landing",
        "runner": "airflow"
    }
)

wait >> fact_visit


#  FACT_lgd_companies_company_partnership
fact_search = BigQueryExecuteQueryOperator(
    task_id='fact_search',
    dag=dag,
    sql=get_sql_string(dags, 'source/sql/dwh/L3/visibility/lgd_companies_company_partnership.sql'),
    destination_dataset_table='logee-data-prod.L3_visibility.fact_lgd_companies_company_partnership',
    write_disposition='WRITE_APPEND',
    allow_large_results=True,
    use_legacy_sql=False,
    location='asia-southeast2',
    schema_update_options=[
        "ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"
    ],
    time_partitioning={
        "type": "DAY"
    },
    labels={
        "type": "scheduled",
        "level": "landing",
        "runner": "airflow"
    }
)

wait >> fact_search
