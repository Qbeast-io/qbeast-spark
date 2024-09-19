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
def process_table(
    table_path: str, revision_id: str
) -> tuple[dict, dict, Cube, list[dict]]:
    """
    Process a delta table given a path to a Qbeast table and a revision ID.
    Example json log file content:
        {"metaData":
            {"id":"d8ed5bdc-9625-4cf2-940e-3ffe0a36104b",
            "format":
                {"provider":"parquet",
                "options":{}},
                "schemaString":
                    "{\"type\":\"struct\",\"fields\":[
                        {\"name\":\"event_time\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},
                        ...}}
        {"add":
            "size":814021,
            "stats":"{
                "tags":
                    {
                        "revision":"1",
                        "blocks":"[
                            {\"cubeId\":\"wggg\",\"minWeight\":-103008559,\"maxWeight\":2147483647,\"elementCount\":9521,\"replicated\":false}
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
        delta_table = DeltaTable(table_path)
        return delta_table

    except Exception as e:
        # If the table doesn't exist, throw an exception
        print(f"Failed when creating the delta table: {e}\n")
        raise e


# 2. extract metadata from delta table
# Returns metadata (dict), dimension_count (int) and symbol_count (int)
def extract_metadata_from_delta_table(
    delta_table: DeltaTable, revision_id: str
) -> (dict, int):
    """
    Extract metadata from a Delta table by checking if the revision ID is in that table.
    If that ID exists in the table, a revision key (e.g. qbeast.revision.1) is created and the metadata is returned.
    It also calculates the number of symbols used to representate the visualization.
    :param delta_table: a created delta table.
    :param revision_id: Configuration entry for the target RevisionID. e.g. 1
    :return: A dictionary with the metadata related to the revisionId, an integer
    representing the number of simbols used for the visualization representation.
    """

    # List with all the revision ids
    metadata = delta_table.metadata().configuration #This returns a dictionary with the metadata
    revision_key = "qbeast.revision." + revision_id

    if revision_key in metadata.keys():
            metadata_key_str = metadata[revision_key]
            metadata_key = json.loads(metadata_key_str)
            dimension_count = len(metadata_key["columnTransformers"])
            symbol_count = (dimension_count + 5) // 6
            print(symbol_count)
            return metadata_key, symbol_count
    else:
        print(f"No metadata found for the given RevisionID. There is no Qbeast table.")
        return None

# 3. Returns a dictionary, the keys are the cubeids and the values are the actual cubes
def extract_cubes_from_delta_table(delta_table: DeltaTable, symbol_count: int) -> dict:
    """
    Takes all the blocks stored in the table and creates the cubes.
    :param delta_table: list of json log files
    :param symbol_count: Integer representing the number of symbols used for the representation
    :return: A dictionary with all the cubes stored in the table.
    """

    df = delta_table.get_add_actions(True).to_pandas()

    df_filtered = df[["size_bytes", "tags.blocks"]]
    # In this dictionary we store a cube for every cubeId. Each cube will be associated to his cube ID
    cubes_dict = dict()

    # Iterate throughout each row of the dataframe
    for index, row in df_filtered.iterrows():
        blocks_str = row[
            "tags.blocks"
        ]  # This returns a pandas series with the blocks of the cube in each of the iterated rows
        # Check if the cube is a JSON chain & try to convert it into a list of dictionaries
        if isinstance(blocks_str, str):
            try:
                blocks = json.loads(
                    blocks_str
                )  # Convert the string JSON into a list of dictionaries
                for block in blocks:
                    # Check if we already have a cube in the dictionary with the cube ID of this block
                    # If we already have a cube with this ID, we update the atributes of the cube
                    # Since objects are passed by reference, they can be modified in situ
                    if block["cubeId"] in cubes_dict.keys():
                        dict_cube = cubes_dict[block["cubeId"]]
                        normalized_max_weight = normalize_weight(
                            int(block["maxWeight"])
                        )
                        dict_cube.max_weight = min(max_weight, normalized_max_weight)
                        dict_cube.element_count += int(block["elementCount"])
                        dict_cube.size += int(row["size_bytes"])
                    # If we don't have this cube in the dictionary, a new cube is created
                    else:
                        # Calculate depth of the cube
                        cube_string = block["cubeId"]
                        depth = len(cube_string) // symbol_count
                        # Assign max_weight, min_weight, elemnt_count a value for the new cube
                        max_weight = block["maxWeight"]
                        element_count = block["elementCount"]
                        replicated = block["replicated"]  # Do we have to add it?
                        min_weight = block["minWeight"]  # Do we have to add it?
                        size = int(row["size_bytes"])
                        cubes_dict[cube_string] = Cube(
                            cube_string, max_weight, element_count, size, depth
                        )
            except json.JSONDecodeError:
                print("Error decoding JSON:", blocks_str)
                raise

    return cubes_dict


# 4. build index from the list of cube blocks
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
def delta_nodes_and_edges(cubes: dict, fraction: float = -1.0) -> list[dict]:
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
        sampling_info.update(cube, node["selected"])

    if fraction > 0:
        print(sampling_info)

    return nodes + edges
