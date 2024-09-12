import json
import os
import glob
from typing import Optional
import pandas as pd
from deltalake import DeltaTable
import pyarrow.parquet as pa
from collections import defaultdict
from qviz.cube import Cube, SamplingInfo, normalize_weight


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


######### NEW CODE ############
# Returns metadata (dict), cubes(dict), root(cube), nodes & edges (list[dict])
def process_table(table_path: str, revision_id: str) -> (dict, dict, Cube, list[dict]):
    """
    Process a delta table given a path to a Qbeast table and a revision ID.
    :param table_path: path to a Qbeast table
    :param revision_id: Configuration entry for the target RevisionID. e.g. 1
    :return: a dictionary with the metadata of the table, a dictionary with all the created cubes, the root node (a cube) of the viusalization and a list of dictionaries with the nodes and edges of the visualization.
    """
    
    # Create the Delta table
    delta_table = create_delta_table(table_path)

    # Extract metadata
    metadata, symbol_count = extract_metadata_from_delta_table(delta_table, revision_id)

    # Extract cubes
    cubes = extract_cubes_from_blocks(delta_table, symbol_count)

    # Build Tree
    root = populate_tree(cubes)

    # Build Tree
    elements = delta_nodes_and_edges(cubes)

    return metadata, cubes, root, elements


# 1. create delta table
def create_delta_table(table_path: str) -> DeltaTable:
    """
    Creates delta table given a path where a Qbeast table is stored.
    :param table_path: local path to your Qbeast table
    :return: a Delta table
    """
    try:
        # Try to find the table in the provided path 
        deltaTable = DeltaTable(table_path)
        return deltaTable
    
    except Exception as e:
        # If the table doesn't exist, throw an exception
        print(f"Failed when creating the delta table: {e}\n")

# 2. extract metadata from delta table
# Returns metadata (dict), dimension_count (int) and symbol_count (int)
def extract_metadata_from_delta_table(delta_table: DeltaTable, revision_id:str) -> (dict, int): 
    """
    Extract metadata from a Delta table by checking if the revision ID is in that table.
    If that ID exists in the table, a revision key (e.g. qbeast.revision.1) is created and the metadata is returned.
    It also calculates the number of symbols used to representate the visualization.
    :param delta_table: a created delta table.
    :param revision_id: Configuration entry for the target RevisionID. e.g. 1
    :return: A dictionary with the metadata related to the revisionId, an integer representing the number of simbols used fot the visualization representation.
    """

    # We create a dataframe with all the add actions
    df = delta_table.get_add_actions(True).to_pandas()
    # List with all the revision ids
    revision_ids = [rev_id.strip() for rev_id in df["tags.revision"]]  # Delete blank spaces in each id
    revision_id = str(revision_id).strip()  # Delete blank spaces in revision_key

    # Check if our table has a revision id that matches the given revision id
    if revision_id in revision_ids:
        print("Metadata found for the given RevisionID\n")
        revision_key = f"qbeast.revision.{revision_id}"
        metadata_str = delta_table.metadata().configuration[revision_key]
        # The last command return a string, and we need a dict to access columnTransformers
        metadata = json.loads(metadata_str)
        # Calculate the number of symbols used for the visualization
        dimension_count = len(metadata['columnTransformers'])
        symbol_count = (dimension_count + 5) // 6
        return metadata, symbol_count
    else:
        print(f"No metadata found for the given RevisionID.")
        symbol_count = float('inf')
        # Create a dataframe with all add actions
        df = delta_table.get_add_actions(True).to_pandas()
        # Convert all blocks JSON chains to a list of dictionaries
        df['tags.blocks'] = df['tags.blocks'].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
        # Make sure that 'tags.blocks' is a list of dictionaries
        exploded_df = df.explode('tags.blocks')
        # Extract 'cubeId'
        all_cube_ids = exploded_df['tags.blocks'].apply(lambda x: x['cubeId'] if isinstance(x, dict) and 'cubeId' in x else None).dropna().tolist()

        for cube_id in all_cube_ids:
            cube_encoding_size = len(cube_id)
            if 0 < cube_encoding_size < symbol_count:
                symbol_count = cube_encoding_size
        return None, symbol_count


# 3. Returns a dictionary, the keys are the cubeids and the values are the actual cubes
def extract_cubes_from_blocks(delta_table: DeltaTable , symbol_count: int) -> dict:
    """
    Takes all the blocks stored in the table and creates the cubes.
    :param delta_table: list of json log files
    :param symbol_count: Integer representing the number of symbols used for the representation
    :return: A dictionary with all the cubes stored in the table.
    """

    df = delta_table.get_add_actions(True).to_pandas()

    df_filtered = df[['size_bytes', 'tags.blocks']]
    # In this dictionary we store a cube for every cubeId. Each cube will be associated to his cube ID
    cubes_dict = dict()

    # Iterate throughout each row of the dataframe
    for index, row in df_filtered.iterrows():
        cube = row["tags.blocks"] # This returns a pandas series with the blocks of the cube in each of the iterated rows

        # Check if the cube is a JSON chain & try to convert it into a list of dictionaries
        if isinstance(cube, str):
            try:
                cube = json.loads(cube)  # Convert the string JSON into a list of dictionaries
            except json.JSONDecodeError:
                print("Error decoding JSON:", cube)

        for block in cube:
            # Check if we already have a cube in the dictionary with the cube ID of this block
            # If we already have a cube with this ID, we update the atributes of the cube
            # Since objects are passed by reference, they can be modified in situ

            if block['cubeId'] in cubes_dict.keys():
                    dict_cube = cubes_dict[block['cubeId']]
                    normalized_max_weight = normalize_weight(int(block['maxWeight']))
                    dict_cube.max_weight = min(max_weight, normalized_max_weight)
                    dict_cube.element_count += int(block['elementCount'])
                    dict_cube.size += int(row['size_bytes'])
            # If we don't have this cube in the dictionary, a new cube is created
            else:
                # Calculate depth of the cube
                cube_string = block['cubeId']
                depth = len(cube_string) // symbol_count
                # Assign max_weight, min_weight, elemnt_count a value for the new cube
                max_weight = block['maxWeight']
                element_count = block['elementCount']
                replicated = block['replicated'] # Do we have to add it?
                min_weight = block['minWeight'] # Do we have to add it?
                size = int(row['size_bytes'])
                cubes_dict[cube_string] = Cube(cube_string, max_weight, element_count, size, depth) 
        
    return cubes_dict


#4. build index from the list of cube blocks
# Returns the root of the tree (cube)
def populate_tree(cubes: dict) -> Cube:
    """
    Assigns a depth (in terms if a tree viusalization) to each cube and finds the root of the tree.
    It also creates the father - child relationships between these cubes.
    :param cubes: dictionary with the cubes stored in a delta table.
    :return: A cube that represents the root of the visualization tree.
    """

    # Initialize variables. level_cubes is a dictionary of lists, being the key each level of representation and the matching value a list of the cubes in that level.
    max_level = 0
    level_cubes = defaultdict(list)

    # Get the cubes from the values of the dictionary and assign a level (key) depending of the depth of each cube
    cubes = cubes.values()
    for cube in cubes:
        level_cubes[cube.depth].append(cube)
        max_level = max(max_level, cube.depth)

    # Create father - child relationships between the cubes
    for level in range(max_level):
        for cube in level_cubes[level]:
            for child in level_cubes[level + 1]:
                child.link(cube)
                
    root = level_cubes[0][0]
    return root

# Populate Tree: Nodes & Edges (list[dict])
def delta_nodes_and_edges(cubes:dict, fraction: float = -1.0) -> list[dict]:
    """
    Sampling function. 
    :param cubes: dictionary with the cubes stored in a delta table.
    :param fraction: float between 0 and 1, used to select cubes based on their normalized maximum weight
    :return: A list of dictionaries that contains the nodes and their connections.
    """

    nodes = []
    edges = []
    sampling_info = SamplingInfo(fraction)
    for cube in cubes.values():
            print("Cube N&E: ", cube)
            node, connections = cube.get_elements_for_sampling(fraction)
            nodes.append(node)
            edges.extend(connections)
            sampling_info.update(cube, node['selected'])

    if fraction > 0:
        print(sampling_info)

    return nodes + edges