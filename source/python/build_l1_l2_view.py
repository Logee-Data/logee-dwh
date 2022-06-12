from google.cloud import bigquery
from google.cloud.exceptions import NotFound

import os

# Check folder and list all view query
L1_folder = './source/sql/dwh/bq_view/L1'
L2_folder = './source/sql/dwh/bq_view/L2'
all_folder = [L1_folder, L2_folder]
all_views = list()

for root_folder in all_folder:
    view_dict = dict()
    level = root_folder.split('/')[-1]
    view_dict['level'] = level

    dataset_folder = [
        i for i in os.listdir(root_folder)
        if os.path.isdir(os.path.join(root_folder, i))
    ]

    for dataset in dataset_folder:
        view_dict['dataset'] = dataset
        dataset_dir = os.path.join(root_folder, dataset)
        sql_files = [
            i for i in os.listdir(dataset_dir)
            if '.sql' in i
        ]

        for sql_file in sql_files:
            view_name = sql_file.replace('.sql', '')
            sql_file_dir = os.path.join(dataset_dir, sql_file)
            view_dict['view_name'] = view_name
            view_dict['view_sql_file'] = sql_file_dir

            all_views.append(view_dict)

if __name__ == '__main__':

    client = bigquery.Client()

    for view in all_views:
        dataset_id = '_'.join([
            view.get('level').upper(),
            view.get('dataset')
        ])

        table_id = view.get('view_name')

        # Get view query
        with open(view.get('view_sql_file'), "r") as view_sql_file:
            view_query = view_sql_file.read()
            view_sql_file.close()

        view_id = f'logee-data-prod.{dataset_id}.{table_id}'

        # Check and create dataset if not exists
        try:
            client.get_dataset(f'logee-data-prod.{dataset_id}')
        except NotFound:
            dataset = bigquery.Dataset(f'logee-data-prod.{dataset_id}')
            dataset.location = "asia-southeast2"
            client.create_dataset(dataset)
            print(f"Dataset {dataset_id} created..")

        # Check if view exists
        exists = False
        try:
            client.get_table(view_id)  # Make an API request.
            exists = True
        except NotFound:
            pass

        bq_view = bigquery.Table(view_id)
        bq_view.view_query = view_query

        if exists:
            bq_view = client.update_table(bq_view, ['view_query'])
            print(f"Updated view {view_id}..")
        else:
            bq_view = client.create_table(bq_view)
            print(f"Created view {view_id}..")
