import json
import os
import glob
from typing import Optional
import pandas as pd
from deltalake import DeltaTable
import pyarrow.parquet as pa
from collections import defaultdict
from qviz.cube import Cube, SamplingInfo


class Block:
    # Origin file
    file = None
    # List of cubes in each block
    cubes = None

    def __init__(self, file, cubes):
        self.file = file
        self.cubes = cubes


def process_table_delta_log(table_path: str, revision_id: str) -> (list[dict], Optional[dict]):
    """
    Extract valid AddFiles from table's _delta_log
    :param table_path: path to qbeast table
    :param revision_id: target RevisionID
    :return: a list of AddFile dictionaries and optionally the metadata dictionary
    """
    assert revision_id != '0', "The processing of the staging revision is currently not supported."

    log_path = os.path.join(table_path, "_delta_log")

    last_checkpoint_file = glob.glob(os.path.join(log_path, "_last_checkpoint"))

    all_json_files = glob.glob(os.path.join(log_path, "*.json"))
    assert len(all_json_files) != 0, "No json log file is found."

    if len(last_checkpoint_file) == 0:
        result = extract_addFiles("", all_json_files, revision_id)
        # Extract revision metadata
        metadata = extract_revision_metadata("", all_json_files, revision_id)
    else:
        checkpoint_version = extract_checkpoint_version(last_checkpoint_file[0])

        # example checkpoint file
        # table_path/_delta_log/00000000000000000010.checkpoint.parquet
        checkpoint_files = glob.glob(os.path.join(log_path, "*.checkpoint.parquet"))
        for file in checkpoint_files:
            # 00000000000000000010.checkpoint.parquet
            file_name = file.split("/")[-1]
            # 10
            version = int(file_name.split(".")[0])
            if version == checkpoint_version:
                checkpoint_file = file
                break
        else:
            raise Exception(f"No checkpoint file found with version {checkpoint_version}.")

        json_files = []
        # example json file: table_path/_delta_log/00000000000000000010.json
        for file in all_json_files:
            file_name = file.split("/")[-1]
            version = int(file_name.split(".")[0])
            if version > checkpoint_version:
                json_files.append(file)

        result = extract_addFiles(checkpoint_file, json_files, revision_id)
        # Extract revision metadata
        metadata = extract_revision_metadata(checkpoint_file, json_files, revision_id)

    if len(result) == 0:
        raise Exception(f"No revision data available at revisionID={revision_id}")

    return result, metadata


def extract_checkpoint_version(last_checkpoint_file: str) -> int:
    """
    The _last_checkpoint file contains the last checkpoint version,
    and the number of ActionFiles contained in the last parquet checkpoint file.
    example _last_checkpoint file content
    {"version":20,"size":202}
    :param last_checkpoint_file: path to the last checkpoint file
    :return: version of the last checkpoint
    """
    with open(last_checkpoint_file, 'r') as f:
        line = f.readline()
    return json.loads(line)["version"]


def extract_addFiles(checkpoint_file: str, json_files: list[str], revision_id: str) -> list[dict]:
    # extract add files from checkpoint file
    add_files_from_checkpoint = addFiles_from_checkpoint_file(checkpoint_file, revision_id)
    # extract add files from json files
    (add_files_from_json, remove_files_from_json) = addFiles_from_json_log_files(json_files, revision_id)
    # Apply Remove to AddFiles
    return list(filter(
        lambda add_file: add_file['path'] not in remove_files_from_json,
        add_files_from_checkpoint + add_files_from_json)
    )


def addFiles_from_checkpoint_file(checkpoint_file: str, revision_id: str) -> list[dict]:
    """
    Find all valid AddFiles from the checkpoint where their revisionID matches
    the provided value. A AddFile is valid is no RemoveFile exists with the same
    path.
    example checkpoint entry:
    {
    'txn': None,
    'add': {'path': '7d807c65-b3a7-4b74-8cee-9eafb1ded46a.parquet',
        'size': 123310,
        ...
        'tags': [('state', 'FLOODED'),
            ('cube', 'w'),
            ...],
        'stats': '...'},
    'remove': None,
    'metaData': None,
    'protocol': None}
    :param checkpoint_file: path to the last checkpoint file
    :param revision_id: revisionID of the index
    :return: a list of AddFile
    """
    if not checkpoint_file:
        return []

    # read parquet checkpoint file
    log_entry_list = pa.read_table(checkpoint_file).to_pylist()

    add_files_dict = dict()
    for entry in log_entry_list:
        # AddFile entry
        if entry['add']:
            add_file = entry['add']
            tags = dict(add_file['tags'])
            if tags['revision'] == revision_id:
                path = add_file['path']
                add_file['tags'] = tags
                add_files_dict[path] = add_file
    for entry in log_entry_list:
        # RemoveFile entry
        if entry['remove']:
            path = entry['remove']['path']
            add_files_dict.pop(path, None)

    return list(add_files_dict.values())


def addFiles_from_json_log_files(json_files: list[str], revision_id: str) -> (list[dict], set[str]):
    """
    Extract AddFiles and RemoveFiles from all json log files
    :param json_files: list, a list of json log file paths
    :param revision_id: target revisionID
    :return: a list of AddFile and a set of RemoveFile paths
    """
    add_files = list()
    remove_paths = set()
    for file in json_files:
        (adds, removes) = load_single_log_file(file, revision_id)
        add_files.extend(adds)
        remove_paths = remove_paths.union(removes)

    return add_files, remove_paths


def load_single_log_file(file_path: str, revision_id: str) -> (list[dict], set[str]):
    """
    Extract valid AddFiles from a single json log file. An AddFile is valid
    if there's no RemoveFile with the same path.
    example log file entries:
    {"metaData":{"id":"c109cfb6-a770-4c02-b004-d44bbe91e981", ...}}
    {"add":{"path":"b4542891-5b03-40cc-8ef1-293493e21814.parquet", ...}}
    {"remove":{"path":"272bbd79-2bf0-444d-b15e-8e4326c9d281.parquet", ...}}
    :param file_path: json log file path
    :param revision_id: target RevisionID
    :return: a list of AddFiles, and a set of RemoveFile paths
    """
    with open(file_path, 'r') as f:
        lines = f.readlines()

    add_files = list()
    remove_paths = set()
    for line in lines:
        action_file = json.loads(line)
        # AddFile entry
        if 'add' in action_file:
            add_file = action_file['add']
            _id = add_file['tags']['revision']
            if _id == revision_id:
                # Add revision AddFile to result
                add_files.append(add_file)
        # RemoveFile entry
        elif 'remove' in action_file:
            remove_file = action_file['remove']
            remove_paths.add(remove_file['path'])

    return add_files, remove_paths


def extract_revision_metadata(checkpoint_file: str, json_files: list[str], revision_id: str) -> dict:
    """
    Extract qbeast metadata configuration entry in the _delta_log/ that contains the target revision.
    Example revision metadata:
    {'revisionID': 1,
    'timestamp': 1675942792996,
    'tableID': '/tmp/test1/',
    'desiredCubeSize': 10000,
    'columnTransformers': ...
    'transformations': ...
    }
    :param checkpoint_file: checkpoint_file if there's any, otherwise the param is left empty: ""
    :param json_files: list of json log files after the checkpoint
    :param revision_id: target RevisionID
    :return: qbeast metadata configuration for the target RevisionID
    """

    revision_key = f"qbeast.revision.{revision_id}"
    # Try to extract the target revision metadata, if any, from the checkpoint file
    metadata_configuration = extract_metadata_from_checkpoint(checkpoint_file, revision_key)

    return metadata_configuration or extract_metadata_from_json_files(json_files, revision_key)

    # Try to extract the target revision metadata, if any, from the checkpoint file
    revision_key = f"qbeast.revision.{revision_id}"
    deltaTable = DeltaTable(checkpoint_file)
    revision_meta = deltaTable.metadata().configuration[revision_key]
    return revision_meta or extract_metadata_from_json_files(json_files, revision_key)

def extract_metadata_from_checkpoint(checkpoint_file: str, revision_key: str) -> Optional[dict]:
    """
    Extract target revision metadata from the checkpoint_file
    :param checkpoint_file: path to the latest checkpoint_file
    :param revision_key: Configuration entry for the target RevisionID. e.g. qbeast.revision.1
    :return: optional metadata configuration for the target RevisionID
    """
    if not checkpoint_file:
        return None

    log_entry_list = pa.read_table(checkpoint_file).to_pylist()
    for record in log_entry_list:
        if record['metaData']:
            configuration_dict = dict(record['metaData']['configuration'])
            if revision_key in configuration_dict:
                return json.loads(configuration_dict[revision_key])
    return None

    if not checkpoint_file:
        return None
    
    deltaTable = DeltaTable(checkpoint_file)
    if revision_key in deltaTable.metadata():
        metadata = deltaTable.metadata().configuration[revision_key]
        return metadata
    return None


def extract_metadata_from_json_files(file_path: str, json_files: list[str], revision_key: str, table_path: str,) -> Optional[dict]:
    """
    Extract target revision metadata from json log files. Only the first line from the file is read.
    Example json log file content with only three lines:
    {'metadata': ...}
    {'add': ...}
    {'commitInfo': ...}
    :param json_files: list of json log files
    :param revision_key: Configuration entry for the target RevisionID. e.g. qbeast.revision.1
    :return: optional metadata configuration for the target RevisionID
    """
    for file in json_files:
        with open(file, 'r') as f:
            lines = f.readlines()

        for line in lines:
            log_in_json = json.loads(line)
            if 'metaData' in log_in_json:
                configuration = log_in_json['metaData']['configuration']
                if revision_key in configuration:
                    return json.loads(configuration[revision_key])
                
    # Extract metadata from json files using DeltaTable       
    deltaTable = DeltaTable(table_path)
    if revision_key in list( deltaTable.get_add_actions(True).to_pandas()["metadata.configuration"] ):
        metadata = deltaTable.metadata().configuration[revision_key]

    return None


## CÓDIGO NUEVO 

def process_table(table_path: str, revision_key: str) -> (cube, list[dic]):
    

    #4. build index from the list of cube blocks
    # Populate Tree, Root
    max_level = 0
    level_cubes = defaultdict(list)
    for block in blocks:
        for cube in block.cube:
            level_cubes[cube.depth].append(cube)
            max_level = max(max_level, cube.depth)

    for level in range(max_level):
        for cube in level_cubes[level]:
            for child in level_cubes[level + 1]:
                child.link(cube)
                
    root = level_cubes[0][0]

    # Populate Tree, Nodes & Edges
    nodes = []
    edges = []
    fraction = -1.0
    sampling_info = SamplingInfo(fraction)
    for block in blocks:
        for cube in block.cube:
            node, connections = cube.get_elements_for_sampling(fraction)
            nodes.append(node)
            edges.extend(connections)
            sampling_info.update(cube, node['selected'])

    if fraction > 0:
        print(sampling_info)

        return root, nodes + edges


def create_delta_table(table_path: str) -> DeltaTable:

    #1. create delta table
    deltaTable = DeltaTable(table_path)

def extract_metadata_from_delta_table(delta_table: DeltaTable, revision_key:str):

    #2. extract metadata from delta table
    if revision_key in list(delta_table.get_add_actions(True).to_pandas()["metadata.configuration"]):
        metadata = delta_table.metadata().configuration[revision_key]
        dimension_count = len(metadata['columnTransformers'])
        symbol_count = (dimension_count + 5) // 6
    else:
        print(f"No metadata found for the given RevisionID.")
        symbol_count = float('inf')
        for add_file in list(delta_table.get_add_actions(True).to_pandas()["add"]):
            cube_encoding_size = len(add_file['tags']['blocks']['cubeId'])
            if 0 < cube_encoding_size < symbol_count:
                symbol_count = cube_encoding_size

def extract_blocks_from_delta_table(delta_table: DeltaTable):

    #3. extract blocks from delta table
    # We get all the adds, and for each file we store its blocks. Then we create the cubes stored in these blocks
    add_list = list( delta_table.get_add_actions(True).to_pandas()["add"] )
    blocks = list( delta_table.get_add_actions(True).to_pandas().groupBy("cubeId")["tags"]['blocks'] )

def extract_cubes_from_blocks():

    for block in blocks:
        cube_string = block[0]['cubeId']
        depth = len(cube_string) // symbol_count
        size = 0
        max_weight = float("inf")
        element_count = 0
        for add in add_list:
            max_weight = min(max_weight, int(add['maxWeight']))
            element_count += int(block['elementCount'])
            size += int(block['size'])

        cubes.append( Cube(cube_string, max_weight, element_count, size, depth) )
