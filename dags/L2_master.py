from os import listdir
from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.time_delta_sensor import TimeDeltaSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor

import pathlib
import yaml

all_jobs = list()

dags = str(pathlib.Path(__file__).parent.absolute())
dataset_folder = [
    '/'.join([f'{dags}/config/L2', f]) for f in listdir(f'{dags}/config/L2') if '.' not in f
]

for i in dataset_folder:
    config_files = [
        '/'.join([i, f]) for f in listdir(i) if ('yaml' in f or 'yml' in f)
    ]

    for j in config_files:

        with open(j, 'r') as yaml_file:
            config_dict = yaml.safe_load(yaml_file)
            yaml_file.close()

        with open(f"{dags}/{config_dict.get('sql')}", 'r') as sql_file:
            sql = sql_file.read()
            sql_file.close()

        skeleton = j.replace('./config/', '').replace('.yaml', '').split('/')

        config_dict['dag_id'] = config_dict.get('dag_id')
        config_dict['sql'] = sql

        all_jobs.append(config_dict)

# Generate DAG
global dag_list
dag_list = dict()

default_args = {
    'owner': 'data-think-tank',
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 12, 31, 17)
}

for job in all_jobs:

    dag_id = job.get('dag_id')

    if dag_list.get(dag_id) is None:
        dag_list[dag_id] = DAG(
            dag_id=dag_id,
            schedule_interval=job.get('schedule'),
            default_args=default_args,
            catchup=False
        )

    wait = TimeDeltaSensor(
        task_id='wait_for_data',
        dag=dag_list[dag_id],
        delta=timedelta(minutes=5)
    )

    dependency_list = list()

    for i in job.get('depends_on'):
        external_dag_id = i.get('dag_id')
        external_task_name_list = i.get('task_id')

        for external_task in external_task_name_list:
            task_id = f'{external_dag_id}_{external_task}'
            external_task = ExternalTaskSensor(
                task_id=f'wait_{task_id}',
                dag=dag_list[dag_id],
                external_dag_id=external_dag_id,
                external_task_id=external_task
            )

            wait >> external_task
            dependency_list.append(external_task)

    l2_sql_run_operator = BigQueryExecuteQueryOperator(
        task_id='move_L1_to_L2',
        dag=dag_list[dag_id],
        sql=job.get('sql'),
        destination_dataset_table=job.get('destination'),
        write_disposition='WRITE_APPEND',
        allow_large_results=True,
        use_legacy_sql=False,
        location='asia-southeast2',
        schema_update_options=[
            "ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"
        ],
        time_partitioning={
            "type": "DAY",
            "field": job.get('time_partitioning')
        },
        labels={
            "type": "scheduled",
            "level": "landing",
            "runner": "airflow"
        }
    )

    for i in dependency_list:
        i >> l2_sql_run_operator

for _dag in dag_list:
    globals()[_dag] = dag_list.get(_dag)
