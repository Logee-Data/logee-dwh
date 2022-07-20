from typing import Dict, List
from google.cloud import bigquery
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.client import Client as BigQueryClient
from google.cloud.bigquery.table import Table as BigQueryTable
from google.api_core.exceptions import NotFound as NotFoundException

import os
import json
import yaml
import click
import copy

class Table:
    def __init__(
        self,
        table_id: str,
        floor: str,
        subfloor: str,
        name: str,
        columns_defined_list: dict,
        inheritances_info: List[Dict] = [],
    ):
        self.table_id = table_id
        self.floor = floor # ex: L1
        self.subfloor = subfloor # ex: visibility
        self.name = name # ex: dma_logee_user

        print(f"initiating recursive walk for config file table columns: {table_id}")
        self.columns_defined: Dict[str, Dict] = self.convert_list_based_columns_to_dict_based(
            list_based_columns=columns_defined_list,
        )

        self.inheritances_info = inheritances_info
        self.columns_inherited: Dict[Dict] = None
        self.is_inheritances_determined: bool = False

    def convert_list_based_columns_to_dict_based(
        self,
        list_based_columns: List[Dict],
    ) -> Dict[str, Dict]:
        dict_based_columns: Dict[str, Dict] = dict()
        for column in list_based_columns:
            name: str = column['name']
            description: str = column['description']
            dict_based_columns[name] = dict(
                name=name,
                description=description,
            )
            if 'columns' in column:
                dict_based_columns[name]['columns'] = self.convert_list_based_columns_to_dict_based(
                    list_based_columns=column['columns'],
                ) # call next recursive
            else:
                pass # at the end of recursive

        return dict_based_columns

    def get_columns_all(self) -> Dict[str, Dict]:
        columns_all: dict = {}
        for column_name, column_defined in self.columns_defined.items():
            columns_all[column_name] = column_defined
        if self.columns_inherited != None:
            for column_name, column_inherited in self.columns_inherited.items():
                columns_all[column_name] = column_inherited
        return columns_all

    def determine_inheritance(self, tables_by_id: dict):
        columns_inherited: Dict[str, Dict] = dict()
        for inheritance_info in self.inheritances_info:
            full_copy: bool = inheritance_info['full_copy']
            inheritance_table_id: str = inheritance_info['table']
            inheritance_info_columns: List[Dict] = inheritance_info['columns']
            referred_inheritance_table: Table = tables_by_id[inheritance_table_id]
            referred_columns: Dict[str, Dict] = referred_inheritance_table.get_columns_all()
            if full_copy: # will inherit all columns from parent. columns key from config will be ignored
                
                # for referred_column_name, referred_column in referred_inheritance_table.columns_defined.items():
                #     referred_column_description: str = referred_column['description']
                #     columns_inherited[referred_column_name] = dict(
                #         description=referred_column_description,
                #     )
                #     # TODO: handle nested struct column
                for referred_column_name, referred_column in referred_columns.items():
                    columns_inherited[referred_column_name] = referred_column
            else:
                # add columns defined in config
                # for inheritance_column in inheritance_info_columns:
                #     inheritance_column_name: str = inheritance_column['name']
                #     referred_column_description: str = referred_inheritance_table.columns_defined[inheritance_column_name]['description']
                #     columns_inherited[inheritance_column_name] = dict(
                #         description=referred_column_description,
                #     )
                #      # TODO: handle nested struct column
                columns_inherited_atomic = self.determine_inheritance_walk(
                    inheritance_info_columns=inheritance_info_columns,
                    referred_columns=referred_columns,
                ) # recursive initiate here
                for walked_column_name, walked_column in columns_inherited_atomic.items():
                    columns_inherited[walked_column_name] = walked_column

        # print(f"debug {self.table_id} columns_inherited", columns_inherited)
        self.columns_inherited: Dict[str, Dict] = columns_inherited
        self.is_inheritances_determined: bool = True

    def determine_inheritance_walk(
        self,
        inheritance_info_columns: List[Dict], # list of column dict taken directly from config file
        referred_columns: Dict[str, Dict], # list of column ON THE SAME LEVEL with inheritance_info_columns
    ) -> Dict[str, Dict]: # key = column name str, value = dict of name,desc,columns
        columns_inherited_atomic: Dict[str, Dict] = dict()
        for inheritance_info_column in inheritance_info_columns:
            inheritance_info_column_name: str = inheritance_info_column['name']
            inheritance_info_column_columns_child: List[Dict] = inheritance_info_column['columns'] \
                if 'columns' in inheritance_info_column \
                else None
            assert inheritance_info_column_name in referred_columns, \
                f"cannot find column {inheritance_info_column_name} (defined at config inherit) at referred columns at the same level {referred_columns}"
            columns_inherited_atomic[inheritance_info_column_name]: Dict = dict(
                name=inheritance_info_column_name,
                description=referred_columns[inheritance_info_column_name]['description'],
            ) # copy happens here
            if inheritance_info_column_columns_child != None:
                # recursive walk child
                assert 'columns' in referred_columns[inheritance_info_column_name], \
                    f"cannot find children columns (key 'columns') for {inheritance_info_column_name} at referred columns {inheritance_info_column}"
                columns_inherited_atomic[inheritance_info_column_name]['columns']: Dict[str, Dict] = self.determine_inheritance_walk(
                    inheritance_info_columns=inheritance_info_column_columns_child,
                    referred_columns=referred_columns[inheritance_info_column_name]['columns'],
                )
            else:
                columns_inherited_atomic[inheritance_info_column_name]['columns'] = None
        return columns_inherited_atomic

    def is_ready_to_determine_inheritance(
        self,
        tables_by_id: dict,
    ) -> bool:
        ready: bool = True
        for inheritance_info in self.inheritances_info:
            inheritance_info: dict
            inheritance_table_id: str = inheritance_info['table']
            if not (inheritance_table_id in tables_by_id):
                raise Exception(f"cannot find table with {inheritance_table_id} that is inherited by {self.table_id}")

            if not (tables_by_id[inheritance_table_id].is_inheritances_determined):
                # parent of this table is not determined
                return False

        return ready

    # def __str__(self) -> str:
    #     return f"{self.table_id} | is_inheritances_determined = {self.is_inheritances_determined} | cols def = {self.columns_defined} | cols inh = {self.columns_inherited}"
        # | parents = {','.join([info.table_id for info in self.inheritances_info])}

    def construct_bigquery_table_address(
        self,
        bigquery_table_address_format: str,
        bigquery_data_project: str,
    ) -> str:
        bigquery_table_address: str = bigquery_table_address_format.format(
            project=bigquery_data_project,
            floor=self.floor,
            subfloor=self.subfloor,
            name=self.name,
        )
        return bigquery_table_address

def do_sync_data_catalog(
    parameters_path: str,
    service_account_path: str,
    dryrun: bool,
):
    this_script_dir: str = \
        os.path.dirname(os.path.realpath(__file__))

    # parameters_folder: str = os.path.join(
    #     os.path.dirname(
    #         this_script_dir
    #     ),
    #     'parameters',
    # )
    parameters: dict = read_yaml(
        file_path=parameters_path,
    )
    print(f"parameters: {parameters}")
    allow_schema_null_or_undefined: bool = parameters['allow_schema_null_or_undefined']
    bigquery_data_project: str = parameters['bigquery_data_project']
    bigquery_table_address_format: str = parameters['bigquery_table_address_format']
    bigquery_job_project: str = parameters['bigquery_job_project']
    bigquery_job_location: str = parameters['bigquery_job_location']

    root_folder: str = os.path.dirname(
        os.path.dirname(
            os.path.dirname(
                this_script_dir
            ),
        ),
    )
    config_folder_path: str = os.path.join(
        root_folder,
        'dags',
        'config',
    )

    print(f"determined project root folder: {root_folder}")
    print(f"determined config folder: {config_folder_path}")

    # scan floor. ex: L1, L2
    floor_folders: list = scan_floor_folders(
        config_folder_path=config_folder_path,
    )

    # scan subfloor. ex: visibility
    for floor_folder in floor_folders:
        floor_folder: dict
        floor_folder['subfloor_folders'] = scan_subfloor_folders(
            floor_folder_path=floor_folder['floor_folder_path'],
        )

    # scan config file. ex: lgd_users.yaml, lgd_orders.yaml, ...
    for floor_folder in floor_folders:
        floor_folder: dict
        for subfloor_folder in floor_folder['subfloor_folders']:
            subfloor_folder: dict
            subfloor_folder['config_files'] = scan_config_files(
                subfloor_folder_path=subfloor_folder['subfloor_folder_path'],
            )

    # read all config files
    for floor_folder in floor_folders:
        floor_folder: dict
        for subfloor_folder in floor_folder['subfloor_folders']:
            subfloor_folder: dict
            for config_file in subfloor_folder['config_files']:
                config_file: dict
                config_file['config'] = read_config_file(
                    config_file_path=config_file['config_file_path'],
                )

    # print(f"config scan result: {json.dumps(floor_folders, indent=2)}")

    # validate local schema: structure
    found_local_invalid_schema: bool = False
    for floor_folder in floor_folders:
        for subfloor_folder in floor_folder['subfloor_folders']:
            for config_file in subfloor_folder['config_files']:
                table_id: str = f"{floor_folder['floor_folder_name']}.{subfloor_folder['subfloor_folder_name']}.{config_file['table_name']}"
                schema_validation_result: dict = validate_local_schema(
                    schema=config_file['config']['schema'],
                    allow_schema_null_or_undefined=allow_schema_null_or_undefined,
                    table_id=table_id,
                )
                if not schema_validation_result['is_valid']:
                    found_local_invalid_schema = True
                    print((
                        f"found invalid schema: {table_id} | "
                        f"reasons: {', '.join(schema_validation_result['invalid_reasons'])}"
                    ))
    if found_local_invalid_schema:
        raise Exception("invalid schema exist. please review the log above")

    # derive schema and inheritance
    table_representation_result: dict = represent_tables(
        floor_folders=floor_folders,
    )

    # push metadata to bigquery
    tables_by_id: Dict[str, Table] = table_representation_result['tables_by_id']
    compare_sync_bigquery_tables_metadata(
        tables_by_id=tables_by_id,
        bigquery_data_project=bigquery_data_project,
        bigquery_table_address_format=bigquery_table_address_format,
        bigquery_job_project=bigquery_job_project,
        bigquery_job_location=bigquery_job_location,
        dryrun=dryrun,
    )

def compare_sync_bigquery_tables_metadata(
    tables_by_id: dict,
    bigquery_data_project: str,
    bigquery_table_address_format: str,
    bigquery_job_project: str,
    bigquery_job_location: str,
    dryrun: bool,
):
    """
    Ref: https://cloud.google.com/bigquery/docs/samples/bigquery-get-table#bigquery_get_table-python
    """
    # Construct a BigQuery client object.
    bigquery_client: BigQueryClient = bigquery.Client()
    # for table_name, table in tables_by_id.items():
    #     table: Table
    #     bigquery_table_address: str = table.construct_bigquery_table_address(
    #         bigquery_table_address_format=bigquery_table_address_format,
    #         bigquery_data_project=bigquery_data_project,
    #     )
    #     print('bigquery_table_address', bigquery_table_address)

    target_tables: Dict[str, Dict] = dict() # including table represented in config and table from bigquery
    target_tables_not_found: Dict[str, Dict] = dict()
    for represented_table_id, represented_table in tables_by_id.items():
        bigquery_table_address: str = represented_table.construct_bigquery_table_address(
            bigquery_table_address_format=bigquery_table_address_format,
            bigquery_data_project=bigquery_data_project,
        )
        
        try:
            print(f"retrieving bigquery table detail for: {bigquery_table_address}")
            bigquery_table: BigQueryTable = bigquery_client.get_table(
                bigquery_table_address
            )
        except NotFoundException as ex:
            target_tables_not_found[represented_table_id] = dict(
                represented_table_id=represented_table_id,
                represented_table=represented_table,
                bigquery_table_address=bigquery_table_address,
            )
            print(f"warning, bigquery table not found for: {bigquery_table_address}")
        else:
            target_tables[represented_table_id] = dict(
                represented_table_id=represented_table_id,
                represented_table=represented_table,
                bigquery_table_address=bigquery_table_address,
                bigquery_table=bigquery_table,
            )

    # print('target_tables', target_tables)
    print("done retrieving all bigquery table details")

    # compare
    for represented_table_id, target in target_tables.items():
        compare_result: dict = compare_representation_and_bigquery_table(
            represented_table_id=target['represented_table_id'],
            represented_table=target['represented_table'],
            bigquery_table_address=target['bigquery_table_address'],
            bigquery_table=target['bigquery_table'],
        )
        target['need_patch'] = compare_result['need_patch']
        target['need_patch_reasons'] = compare_result['need_patch_reasons']
        target['full_patched_fields'] = compare_result['full_patched_fields']

    # validation: all columns still exist (there are no deleted SchemaField)
    # TODO: here

    # print plan
    total_target: int = len(target_tables)
    number_of_target_need_patch: int = len([None for target in target_tables.values() if target['need_patch']])
    number_of_target_already_ok: int = total_target - number_of_target_need_patch
    if number_of_target_need_patch > 0:
        print(f"overview plan: will update {number_of_target_need_patch} out of {total_target} tables")
        for represented_table_id, target in target_tables.items():
            if target['need_patch']:
                reason_log: str = '\n'.join([f"- {reason}" for reason in target['need_patch_reasons']])
                print((
                    f"plan: will update column description for bigquery table: {target['bigquery_table_address']} "
                    f"(config = {represented_table_id}) because: \n"
                    f"{reason_log}"
                    '\n'
                ))
    else:
        print(f"overview plan: will not update any tables, because all {number_of_target_already_ok} from {total_target} tables already have column description as defined in config")

    # sync
    if dryrun:
        print(f"dryrun is {dryrun}, thus, not updating bigquery tables")
    else:
        for represented_table_id, target in target_tables.items():
            if target['need_patch']:
                print(f"execute: updating column description for bigquery table {target['bigquery_table_address']} (config = {represented_table_id})")
                patch_bigquery_table(
                    bigquery_client=bigquery_client,
                    bigquery_table=target['bigquery_table'],
                    full_patched_fields=target['full_patched_fields'],
                )

def patch_bigquery_table(
    bigquery_client: BigQueryClient,
    bigquery_table: BigQueryTable,
    full_patched_fields: List[SchemaField],
):
    bigquery_table.schema = full_patched_fields
    bigquery_client.update_table(bigquery_table, ['schema'])

def compare_representation_and_bigquery_table(
    represented_table_id: str,
    represented_table: Table,
    bigquery_table_address: str,
    bigquery_table: BigQueryTable,
) -> dict:
    represented_columns: Dict[str, Dict] = represented_table.get_columns_all()
    bigquery_columns: List[SchemaField] = bigquery_table.schema
    tracing_breadcrumb_column: str = [f"[{bigquery_table_address}|{represented_table_id}]"]

    # print('debugx1 bigquery_table.schema', bigquery_table.schema)

    compare_walk_result: dict = compare_representation_and_bigquery_columns_walk(
        represented_columns=represented_columns,
        bigquery_columns=bigquery_columns,
        tracing_breadcrumb_column=tracing_breadcrumb_column,
    )
    need_patch: bool = compare_walk_result['need_patch']
    need_patch_reasons: List[str] = compare_walk_result['need_patch_reasons']
    full_patched_fields: List[SchemaField] = compare_walk_result['full_patched_fields']

    # print('debugx1 need_patch', need_patch)
    # print('debugx1 need_patch_reasons', need_patch_reasons)
    # print('debugx1 full_patched_fields', full_patched_fields)
    # print()
    # print()

    return dict(
        need_patch=need_patch,
        need_patch_reasons=need_patch_reasons,
        full_patched_fields=full_patched_fields,
    )

def compare_representation_and_bigquery_columns_walk(
    represented_columns: Dict[str, Dict],
    bigquery_columns: List[SchemaField],
    tracing_breadcrumb_column: List[str], # for logging purpose only
) -> dict:
    need_patch: bool = False
    need_patch_reasons: List[str] = []
    full_patched_fields: List[SchemaField] = []
    full_patched_fields_indexed: Dict[str, SchemaField] = dict() # updated field will get in here first before full_patched_fields for performance reason

    bigquery_columns_indexed: Dict[str, SchemaField] = convert_list_based_schemafields_to_dict_based(
        bigquery_columns=bigquery_columns,
    )

    # iterate columns from config to add to updated fields (exist in config, but exist/not exist in BQ)
    for represented_column_name, represented_column in represented_columns.items():
        if not (represented_column_name in bigquery_columns_indexed):
            print((
                f"warning: skip column because not found in bigquery table: "
                f"{'.'.join(tracing_breadcrumb_column + [represented_column_name])}"
            ))
        else:
            bigquery_column: SchemaField = bigquery_columns_indexed[represented_column_name]
            represented_column_description: str = represented_column['description']
            bigquery_column_description: str = bigquery_column.description
            if represented_column_description != bigquery_column_description:
                need_patch = True
                need_patch_reasons.append((
                    f"different description for column "
                    f"{'.'.join(tracing_breadcrumb_column + [represented_column_name])}: "
                    f"config = '{represented_column_description}' | "
                    f"bigquery = '{bigquery_column_description}'"
                ))
                atomic_full_patched_description: str = represented_column_description
            else:
                atomic_full_patched_description: str = bigquery_column.description # already identical

            # walk the childs
            if not ('columns' in represented_column): # the end of recursive
                atomic_full_patched_fields: List[SchemaField] = bigquery_column.fields
            elif represented_column['columns'] == None: # the end of recursive
                atomic_full_patched_fields: List[SchemaField] = bigquery_column.fields
            elif len(represented_column['columns']) == 0: # the end of recursive
                atomic_full_patched_fields: List[SchemaField] = bigquery_column.fields
            else:
                deeper_walk_represented_columns: Dict[str, Dict] = represented_column['columns']
                if bigquery_column.fields == None:
                    print((
                        f"warning: skip child columns because not found in bigquery table: "
                        f"{'.'.join(tracing_breadcrumb_column + [represented_column_name])}"
                    ))
                    atomic_full_patched_fields: List[SchemaField] = bigquery_column.fields
                else:
                    deeper_walk_bigquery_columns: List[SchemaField] = bigquery_column.fields
                    deeper_walk_compare_result: dict = compare_representation_and_bigquery_columns_walk(
                        represented_columns=deeper_walk_represented_columns,
                        bigquery_columns=deeper_walk_bigquery_columns,
                        tracing_breadcrumb_column=(tracing_breadcrumb_column + [represented_column_name]),
                    )
                    if deeper_walk_compare_result['need_patch']:
                        need_patch = True
                        need_patch_reasons += deeper_walk_compare_result['need_patch_reasons']
                        atomic_full_patched_fields: List[SchemaField] = deeper_walk_compare_result['full_patched_fields']
                    else:
                        atomic_full_patched_fields: List[SchemaField] = bigquery_column.fields
        
            atomic_full_patched_field: SchemaField = SchemaField(
                name=bigquery_column.name,
                field_type=bigquery_column.field_type,
                mode=bigquery_column.mode,
                description=atomic_full_patched_description,
                fields=atomic_full_patched_fields,
                policy_tags=bigquery_column.policy_tags,
                precision=bigquery_column.precision,
                scale=bigquery_column.scale,
                max_length=bigquery_column.max_length,
            )
            full_patched_fields_indexed[bigquery_column.name] = atomic_full_patched_field

    # iterate columns from bigquery to add to updated fields (exist in BQ, not exist in config)
    for bigquery_column in bigquery_columns:
        bigquery_column: SchemaField
        if bigquery_column.name in full_patched_fields_indexed:
            pass # exist in BQ and exist in config
        else: # exist in BQ but not exist in config
            print((
                f"warning: the following column exist in BigQuery but not exist in config: "
                f"bigquery column name = {'.'.join(tracing_breadcrumb_column + [bigquery_column.name])}"
            ))
            full_patched_fields_indexed[bigquery_column.name] = bigquery_column

    # convert dict based full_patched_fields to list
    for _, field in full_patched_fields_indexed.items():
        full_patched_fields.append(field)

    return dict(
        need_patch=need_patch,
        need_patch_reasons=need_patch_reasons,
        full_patched_fields=full_patched_fields,
    )

def convert_list_based_schemafields_to_dict_based(
    bigquery_columns: List[SchemaField],
) -> Dict[str, SchemaField]:
    """
    Purpose: to increase performance when searching for column by column name string.
    """
    bigquery_columns_indexed: Dict[str, SchemaField] = dict()
    # key = column name str, value = SchemaField
    for schema_field in bigquery_columns:
        bigquery_columns_indexed[schema_field.name] = schema_field
    return bigquery_columns_indexed

def get_existing_bigquery_table(
    bigquery_table_id: str,
    bigquery_client: BigQueryClient,
):
    bigquery_table: BigQueryTable = bigquery_client.get_table(
        bigquery_table_id
    )

    # View table properties
    # print('type(table)', type(table))
    # print(
    #     "Got table '{}.{}.{}'.".format(table.project, table.dataset_id, table.table_id)
    # )
    # print("Table schema: {}".format(table.schema))
    # print("Table description: {}".format(table.description))
    # print("Table has {} rows".format(table.num_rows))

    # table_schema: List[SchemaField] = table.schema

    # print('debug table', table)
    # print('debug table_schema', table_schema)
    # print('type(table)', type(table))
    # full_updated_fields: List[SchemaField] = []
    # for bigquery_schema_field in table_schema:
    #     # fields: List[SchemaField] = []
    #     if bigquery_schema_field.name == 'user_meta':
    #         fields_user_meta: List[SchemaField] = []
    #         for bigquery_schema_field_user_meta in bigquery_schema_field.fields:
    #             # print('debug2 bigquery_schema_field_user_meta', bigquery_schema_field_user_meta)
    #             if bigquery_schema_field_user_meta.name == 'phone':
    #                 print('debug3 bigquery_schema_field_user_meta.description', bigquery_schema_field_user_meta.description)
    #                 fields_user_meta.append(SchemaField(
    #                     name=bigquery_schema_field_user_meta.name,
    #                     field_type=bigquery_schema_field_user_meta.field_type,
    #                     mode=bigquery_schema_field_user_meta.mode,
    #                     description='this is just a meta test 2',
    #                     fields=bigquery_schema_field_user_meta.fields,
    #                     policy_tags=bigquery_schema_field_user_meta.policy_tags,
    #                     precision=bigquery_schema_field_user_meta.precision,
    #                     scale=bigquery_schema_field_user_meta.scale,
    #                     max_length=bigquery_schema_field_user_meta.max_length,
    #                 ))
    #             else:
    #                 fields_user_meta.append(bigquery_schema_field_user_meta)
    #         full_updated_fields.append(SchemaField(
    #             name=bigquery_schema_field.name,
    #             field_type=bigquery_schema_field.field_type,
    #             mode=bigquery_schema_field.mode,
    #             description=bigquery_schema_field.description,
    #             fields=fields_user_meta,
    #             policy_tags=bigquery_schema_field.policy_tags,
    #             precision=bigquery_schema_field.precision,
    #             scale=bigquery_schema_field.scale,
    #             max_length=bigquery_schema_field.max_length,
    #         ))
    #     else:
    #         full_updated_fields.append(bigquery_schema_field)
    # print('debug4 full_updated_fields', full_updated_fields)
    # table.schema = full_updated_fields
    # bigquery_client.update_table(table, ['schema'])

    # update_table_column_description(
    #     bigquery_client=bigquery_client,
    #     bigquery_job_project=bigquery_job_project,
    #     bigquery_job_location=bigquery_job_location,
    # )

# def update_table_column_description(
#     bigquery_client,
#     bigquery_job_project: str,
#     bigquery_job_location: str,
# ):
#     alter_col_desc_query: str = (
#         "ALTER TABLE `logee-data-dev.tmp.l1_orders`"
#         "ALTER COLUMN op_masked \n"
#         "SET OPTIONS (\n"
#         "description=\"the description of op which is masked v2\"\n"
#         ");"
#     )
#     print(f"running query: {alter_col_desc_query}")
#     query_job = bigquery_client.query(
#         query=alter_col_desc_query,
#         project=bigquery_job_project,
#         location=bigquery_job_location,
#     )
#     _ = query_job.result()

def represent_tables(
    floor_folders: list,
) -> dict:
    tables_by_id: Dict[str, Table] = dict() # key = floor.subfloor.table_name

    # represent all tables without dependencies yet
    for floor_folder in floor_folders:
        for subfloor_folder in floor_folder['subfloor_folders']:
            for config_file in subfloor_folder['config_files']:
                table_id: str = f"{floor_folder['floor_folder_name']}.{subfloor_folder['subfloor_folder_name']}.{config_file['table_name']}"

                # create/represent table
                if ('schema' in config_file['config']) and (config_file['config']['schema'] != None):
                    table: Table = Table(
                        table_id=table_id,
                        floor=floor_folder['floor_folder_name'],
                        subfloor=subfloor_folder['subfloor_folder_name'],
                        name=config_file['table_name'],
                        columns_defined_list=config_file['config']['schema']['columns'],
                    )
                    tables_by_id[table_id] = table

                    # set inheritances info
                    schema: dict = config_file['config']['schema']
                    if schema == None:
                        tables_by_id[table_id].is_inheritances_determined = True
                    else:
                        columns: list = schema['columns']
                        inheritances_info: list = schema['inherit']
                        tables_by_id[table_id].inheritances_info = inheritances_info
                        number_of_inheritances_info: int = len(inheritances_info)

                        if number_of_inheritances_info == 0:
                            tables_by_id[table_id].is_inheritances_determined = True
                        else:
                            tables_by_id[table_id].inheritances_info = inheritances_info

    while not all_tables_inheritance_determined(tables_by_id=tables_by_id):
        for table_id, table in tables_by_id.items():
            if table.is_ready_to_determine_inheritance(tables_by_id=tables_by_id):
                table.determine_inheritance(
                    tables_by_id=tables_by_id,
                )

    # TODO: remove debug
    # print('tables_by_id.keys()', tables_by_id.keys())
    # for table_id, table in tables_by_id.items():
    #     print(f"{table_id} | columns_defined = {table.columns_defined} | columns_inherited = {table.columns_inherited}")

    return dict(
        tables_by_id=tables_by_id,
    )

def all_tables_inheritance_determined(tables_by_id: dict) -> bool:
    all_ready: bool = True
    for table_id, table in tables_by_id.items():
        if not (table.is_inheritances_determined):
            return False
    return all_ready

def read_yaml(file_path: str) -> Dict:
    with open(file_path, 'r') as file:
        file_content = file.read()
    loaded_content: Dict = yaml.safe_load(file_content)
    return loaded_content

def scan_floor_folders(config_folder_path: str) -> list:
    floor_folders: list = [
        dict(
            base_path=config_folder_path,
            floor_folder_name=location,
            floor_folder_path=os.path.join(
                config_folder_path,
                location,
            ),
        )
        for location in os.listdir(config_folder_path)
        if os.path.isdir(os.path.join(
            config_folder_path,
            location,
        ))
    ]
    return floor_folders

def scan_subfloor_folders(floor_folder_path: str) -> list:
    subfloor_folders: list = []
    for location in os.listdir(floor_folder_path):
        location_is_dir: bool = os.path.isdir(os.path.join(
            floor_folder_path,
            location,
        ))
        if location_is_dir:
            subfloor_folders.append(dict(
                base_path=floor_folder_path,
                subfloor_folder_name=location,
                subfloor_folder_path=os.path.join(
                    floor_folder_path,
                    location,
                ),
            ))
    return subfloor_folders

def scan_config_files(subfloor_folder_path: str) -> list:
    config_files: list = []
    for location in os.listdir(subfloor_folder_path):
        location_is_yaml_file: bool = os.path.isfile(os.path.join(
            subfloor_folder_path,
            location,
        )) and (location.endswith('.yaml'))
        if location_is_yaml_file:
            config_files.append(dict(
                base_path=subfloor_folder_path,
                config_file_name=location,
                config_file_path=os.path.join(
                    subfloor_folder_path,
                    location,
                ),
                table_name=location.split('.')[0],
            ))
    return config_files

def read_config_file(config_file_path: str) -> dict:
    config_full: dict = read_yaml(
        file_path=config_file_path,
    )
    config: dict = dict()
    if not('schema' in config_full):
        config['schema'] = None
    else:
        config['schema'] = config_full['schema']

    # print('config', config)
    return config

def validate_local_schema(
    schema: dict,
    allow_schema_null_or_undefined: bool,
    table_id: str, # for logging only
) -> dict:
    is_valid: bool = True
    invalid_reasons: List[str] = []

    # schema type should be dictionary
    if schema == None:
        if not allow_schema_null_or_undefined:
            is_valid = False
            invalid_reasons.append("schema is not defined")
        else:
            print(f"warning: schema for {table_id} is not defined in config folder but allowed")
    elif not isinstance(schema, dict):
        is_valid = False
        invalid_reasons.append(f"schema should be a dictionary / map. found: {type(schema)}")
    else:
        # columns should exist
        if not ('columns' in schema):
            is_valid = False
            invalid_reasons.append("field 'columns' not exist in schema")
        else:
            # columns should be list
            if not isinstance(schema['columns'], list):
                is_valid = False
                invalid_reasons.append(f"schema.columns should be a list. found: {type(schema['columns'])}")

        # inherit should exist
        if not ('inherit' in schema):
            is_valid = False
            invalid_reasons.append("field 'inherit' not exist in schema")
        else:
            # inherit should be list
            if not isinstance(schema['inherit'], list):
                is_valid = False
                invalid_reasons.append(f"schema.inherit should be a list. found: {type(schema['inherit'])}")
            else:
                for index, inheritance in enumerate(schema['inherit']):
                    # inherit items should be dict
                    if not isinstance(inheritance, dict):
                        is_valid = False
                        invalid_reasons.append(f"schema.inherit.[{index}] should be a dict. found: {type(inheritance)}")
                    else:
                        # inherit.[].table_id should exist
                        for mandatory_field in ['table', 'full_copy', 'columns']:
                            if not (mandatory_field in inheritance):
                                is_valid = False
                                invalid_reasons.append(f"field '{mandatory_field}' not found in schema.inherit.[{index}]")

    return dict(
        is_valid=is_valid,
        invalid_reasons=invalid_reasons,
    )

@click.group()
def cli():
    pass

@cli.command()
@click.option('--parameters_path', type=str, required=True, help='absolute path to config yaml file')
@click.option('--service_account_path', type=str, required=True, help='absolute path to GCP service account json file')
@click.option('--dryrun', type=bool, required=True, help='whether to dry run (run without executing)')
def sync_data_catalog(
    parameters_path: str,
    service_account_path: str,
    dryrun: bool,
):
    do_sync_data_catalog(
        parameters_path=parameters_path,
        service_account_path=service_account_path,
        dryrun=dryrun,
    )

if __name__ == '__main__': cli()
