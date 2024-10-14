import json
from collections import defaultdict

from deltalake import DeltaTable

from qviz.cube import Cube, Block, SamplingInfo


def process_table(
        table_path: str, revision_id: int
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
    delta_table = DeltaTable(table_path)
    revision = load_revision(delta_table, revision_id)
    cubes = load_revision_cubes(delta_table, revision)
    root = populate_tree(cubes)
    elements = get_nodes_and_edges(cubes)
    return revision, cubes, root, elements


def load_revision(delta_table: DeltaTable, revision_id: int) -> (dict, int):
    config = delta_table.metadata().configuration
    revision_key = f"qbeast.revision.{revision_id}"
    try:
        revision_str = config[revision_key]
        revision = json.loads(revision_str)
        return revision
    except KeyError as e:
        available_revisions = [k for k in config.keys() if k.startswith("qbeast.revision.")]
        print(f"No metadata found for the given RevisionID: {revision_id}. Available revisions: {available_revisions}")
        raise e


def load_revision_cubes(delta_table: DeltaTable, revision: dict) -> dict:
    revision_id_str = str(revision["revisionID"])
    dimension_count = len(revision["columnTransformers"])
    symbol_count = (dimension_count + 5) // 6
    all_cubes = dict()
    add_files = delta_table.get_add_actions(True).to_pylist()
    for add_file in add_files:
        if add_file.get("tags.revision", '-1') != revision_id_str:
            continue
        file_bytes = add_file["size_bytes"]
        file_path = add_file["path"]
        blocks = json.loads(add_file["tags.blocks"])
        for block_dict in blocks:
            cube_id = block_dict["cubeId"]
            if cube_id not in all_cubes:
                depth = len(cube_id) // symbol_count
                cube = Cube(cube_id, depth=depth)
                all_cubes[cube_id] = cube
            cube = all_cubes[cube_id]
            block = Block.from_dict(block_dict, file_path, file_bytes)
            cube.add(block)
    return all_cubes


def populate_tree(cubes: dict) -> Cube:
    max_level = 0
    level_cubes = defaultdict(list)
    cubes = cubes.values()
    for cube in cubes:
        level_cubes[cube.depth].append(cube)
        max_level = max(max_level, cube.depth)
    for level in range(max_level):
        for cube in level_cubes[level]:
            for child in level_cubes[level + 1]:
                child.link(cube)
    root = level_cubes[0][0]
    return root


def get_nodes_and_edges(cubes: dict, fraction: float = -1.0) -> list[dict]:
    nodes = []
    edges = []
    sampling_info = SamplingInfo(fraction)
    for cube in cubes.values():
        node, connections = cube.get_elements_for_sampling(fraction)
        nodes.append(node)
        edges.extend(connections)
        sampling_info.update(cube, node["selected"])
    if fraction > 0:
        print(sampling_info)
    return nodes + edges


# def get_node_and_edges_from_cube(cube: Cube, fraction: float) -> tuple[dict, list[dict]]:
#     node, edges = cube.get_elements_for_sampling(fraction)
#     return node, edges


def get_node_and_edges_from_cube(cube: Cube, fraction: float) -> (dict, list[dict]):
    """
    Return cube and edges for drawing if the cube or any of its children are selected
    for the given sampling fraction.

    :param cube: cube to be drawn
    :param fraction: sampling fraction between [0, 1]
    :return: styled node and edges for drawing depending on if they are selected for
    sampling
    """
    selected = any([b.is_sampled(fraction) for b in cube.blocks])
    name = cube.cube_id or "root"
    label = (name + " " if name == "root" else "") + str(cube.max_weight)
    node = {
        "data": {"id": name, "label": label},
        "selected": selected,
        "classes": "sampled" if selected else "",
    }

    edges = []
    for child in cube.children:
        selected_child = child.is_sampled(fraction)
        edges.append(
            {
                "data": {"source": name, "target": child.cube_id},
                "selected": selected_child,
                "classes": "sampled" if selected_child else "",
            }
        )

    return node, edges