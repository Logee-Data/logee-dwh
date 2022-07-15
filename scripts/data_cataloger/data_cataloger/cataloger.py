import os
import json
import yaml
from typing import Dict, List

class Table:
    def __init__(
        self,
    ):
        pass

def main(
    env: str,
    dryrun: bool,
):
    this_script_dir: str = \
        os.path.dirname(os.path.realpath(__file__))

    parameters_folder: str = os.path.join(
        os.path.dirname(
            this_script_dir
        ),
        'parameters',
    )
    parameters: dict = read_yaml(
        file_path=os.path.join(
            parameters_folder,
            f"parameters.{env}.yaml",
        ),
    )
    print(f"parameters: {parameters}")
    allow_schema_null_or_undefined: bool = parameters['allow_schema_null_or_undefined']

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

    # TODO: derive schema and inheritance
    table_representation_result: dict = represent_tables(
        floor_folders=floor_folders,
    )

    # TODO: push metadata to bigquery

class Table:
    def __init__(
        self,
        table_id: str,
        name: str,
        columns_defined_list: dict,
        inheritances_info: List[Dict] = [],
    ):
        self.table_id = table_id
        self.name = name

        self.columns_defined: Dict[Dict] = dict()
        for column in columns_defined_list:
            self.columns_defined[column['name']] = column

        self.inheritances_info = inheritances_info
        self.is_inheritances_determined: bool = False

    def determine_inheritance(self, tables_by_id: dict):
        columns_inherited: Dict[Dict] = dict()
        for inheritance_info in self.inheritances_info:
            full_copy: bool = inheritance_info['full_copy']
            inheritance_table_id: str = inheritance_info['table']
            inheritance_input_columns: list = inheritance_info['columns']
            if full_copy: # will inherit all columns from parent. columns key from config will be ignored
                # TODO
                # print(f"debug2 {self.table_id} full_copy")
                referred_inheritance_table: Table = tables_by_id[inheritance_table_id]
                for referred_column_name, referred_column in referred_inheritance_table.columns_defined.items():
                    referred_column_description: str = referred_column['description']
                    columns_inherited[referred_column_name] = dict(
                        description=referred_column_description,
                    )
            else:
                # add columns defined in config
                referred_inheritance_table: Table = tables_by_id[inheritance_table_id]
                for inheritance_column in inheritance_input_columns:
                    inheritance_column_name: str = inheritance_column['name']
                    referred_column_description: str = referred_inheritance_table.columns_defined[inheritance_column_name]['description']
                    columns_inherited[inheritance_column_name] = dict(
                        description=referred_column_description,
                    )

        # print(f"debug {self.table_id} columns_inherited", columns_inherited)
        self.columns_inherited: Dict[Dict] = columns_inherited
        self.is_inheritances_determined: bool = True

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

def represent_tables(
    floor_folders: list,
) -> dict:
    tables_by_id: dict = dict() # key = floor.subfloor.table_name

    # represent all tables without dependencies yet
    for floor_folder in floor_folders:
        for subfloor_folder in floor_folder['subfloor_folders']:
            for config_file in subfloor_folder['config_files']:
                table_id: str = f"{floor_folder['floor_folder_name']}.{subfloor_folder['subfloor_folder_name']}.{config_file['table_name']}"

                # create/represent table
                if ('schema' in config_file['config']) and (config_file['config']['schema'] != None):
                    table: Table = Table(
                        table_id=table_id,
                        name=config_file['table_name'],
                        columns_defined_list=config_file['config']['schema']['columns'],
                    )
                    tables_by_id[table_id] = table
                    print('table', table)

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


    print('tables_by_id.keys()', tables_by_id.keys())

    for table_id, table in tables_by_id.items():
        print(table_id, table)

    while not all_tables_inheritance_determined(tables_by_id=tables_by_id):
        for table_id, table in tables_by_id.items():
            if table.is_ready_to_determine_inheritance(tables_by_id=tables_by_id):
                table.determine_inheritance(
                    tables_by_id=tables_by_id,
                )

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
            print(f"warning: schema for {table_id} is not defined but allowed")
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

if __name__ == '__main__':
    main(env='development', dryrun=True) # TODO: click parameterize
