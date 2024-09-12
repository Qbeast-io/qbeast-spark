import json
import os
import glob
from typing import Optional
import pandas as pd
from deltalake import DeltaTable
import pyarrow.parquet as pa
from collections import defaultdict
from qviz.cube import Cube, SamplingInfo, normalize_weight

# Returns metadata (dict), cubes(dict), root(cube), nodes & edges (list[dict])
def process_table(table_path: str, revision_id: str) -> tuple[dict, dict, Cube, list[dict]]:
    """
    Process a delta table given a path to a Qbeast table and a revision ID.
    Example json log file content:
        {"commitInfo":
            {"timestamp":1726134717979,
            "operation":"WRITE",
            "operationParameters":
                {"mode":"Overwrite"},
                "isolationLevel":"Serializable",
                "isBlindAppend":false,
                "operationMetrics":{"numFiles":"16",
                "numOutputRows":"300000",
                "numOutputBytes":"9486957"},
            "engineInfo":"Apache-Spark/3.5.0 Delta-Lake/3.1.0",
            "txnId":"450ebb6f-6845-4e75-a1b5-d1143a1b9f29"}}
        {"metaData":
            {"id":"d8ed5bdc-9625-4cf2-940e-3ffe0a36104b",
            "format":
                {"provider":"parquet",
                "options":{}},
                "schemaString":
                    "{\"type\":\"struct\",\"fields\":[
                        {\"name\":\"event_time\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},
                        {\"name\":\"event_type\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},
                        ...}}
        {"protocol":
            {"minReaderVersion":1,
            "minWriterVersion":2}}
        {"add":
            {"path":"8eb0fcd7-c5ca-4b4c-a967-bc10188be935.parquet",
            "partitionValues":{},
            "size":814021,
            "modificationTime":1726134717043,
            "dataChange":true,
            "stats":"{
                \"numRecords\":27384,
                \"minValues\":{
                    \"event_time\":\"2019-11-01T01:00:08.000+01:00\",
                    \"event_type\":\"cart\",
                    \"product_id\":1002225,
                    \"category_id\":2053013552259662037,
                    \"category_code\":\"accessories.bag\",
                    \"brand\":\"nady\",
                    \"price\":0.0,
                    \"user_id\":529936225,
                    \"user_session\":\"00088e2e-4611-46fa-a7ce-bd1e6f05\"},
                \"maxValues\":{
                    \"event_time\":\"2019-11-01T07:45:50.000+01:00\",
                    \"event_type\":\"view\",
                    \"product_id\":60000025,
                    ...},
                \"nullCount\":{
                    \"event_time\":0,
                    \"event_type\":0,
                    \"product_id\":0,
                    ...}",
                "tags":
                    {
                        "revision":"1",
                        "blocks":"[
                            {\"cubeId\":\"wggg\",\"minWeight\":-103008559,\"maxWeight\":2147483647,\"elementCount\":9521,\"replicated\":false},
                            {\"cubeId\":\"wggQ\",\"minWeight\":-100165008,\"maxWeight\":2147483647,\"elementCount\":880,\"replicated\":false},
                            {\"cubeId\":\"wgg\",\"minWeight\":-1401341541,\"maxWeight\":-103459089,\"elementCount\":9759,\"replicated\":false},
                            {\"cubeId\":\"wggA\",\"minWeight\":-102673026,\"maxWeight\":2147483647,\"elementCount\":5982,\"replicated\":false},
                            {\"cubeId\":\"wggw\",\"minWeight\":-91591261,\"maxWeight\":2147483647,\"elementCount\":1242,\"replicated\":false}]"}}}
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