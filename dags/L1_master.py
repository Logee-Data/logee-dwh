from os import listdir
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryExecuteQueryOperator
)

import yaml

all_jobs = list()

dataset_folder = [
    '/'.join(['./config/L1', f]) for f in listdir('./config/L1') if '.' not in f
]

for i in dataset_folder:
    config_files = [
        '/'.join([i, f]) for f in listdir(i) if ('yaml' in f or 'yml' in f)
    ]

    for j in config_files:

        with open(j, 'r') as yaml_file:
            config_dict = yaml.safe_load(yaml_file)
            yaml_file.close()

        with open(config_dict.get('sql'), 'r') as sql_file:
            sql = sql_file.read()
            sql_file.close()

        skeleton = j.replace('./config/', '').replace('.yaml', '').split('/')

        config_dict['dag_id'] = '_'.join([config_dict.get('dataset_id'), config_dict.get('table_id')])
        config_dict['sql'] = sql

        all_jobs.append(config_dict)

# Generate DAG
global dag_list
dag_list = dict()

default_args = {
    'owner': 'data-think-tank',
    'retry_delay': timedelta(minutes=5)
}

for job in all_jobs:

    dag = DAG(
        dag_id=job.get('dag_id'),
        schedule_interval=job.get('schedule'),
        default_args=default_args,
        catchup=False
    )

