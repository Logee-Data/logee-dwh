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

    print(f"config scan result: {json.dumps(floor_folders, indent=2)}")

    # validate local schema: structure
    found_local_invalid_schema: bool = False
    for floor_folder in floor_folders:
        for subfloor_folder in floor_folder['subfloor_folders']:
            for config_file in subfloor_folder['config_files']:
                table_id: str = f"{floor_folder['floor_folder_name']}.{subfloor_folder['subfloor_folder_name']}.{config_file['config_file_name']}"
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
    for floor_folder in floor_folders:
        for subfloor_folder in floor_folder['subfloor_folders']:
            for config_file in subfloor_folder['config_files']:
                pass

    # TODO: push metadata to bigquery

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
                        for mandatory_field in ['table_id', 'full_copy', 'columns']:
                            if not (mandatory_field in inheritance):
                                is_valid = False
                                invalid_reasons.append(f"field '{mandatory_field}' not found in schema.inherit.[{index}]")

    return dict(
        is_valid=is_valid,
        invalid_reasons=invalid_reasons,
    )

if __name__ == '__main__':
    main(env='development', dryrun=True) # TODO: click parameterize
