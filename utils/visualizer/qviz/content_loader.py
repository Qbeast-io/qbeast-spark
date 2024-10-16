import json
from collections import defaultdict
from deltalake import DeltaTable
from qviz.cube import Cube, Block, SamplingInfo


def process_table(table_path: str, revision_id: int) -> tuple[dict, list[dict]]:
    delta_table = DeltaTable(table_path)
    revision = load_revision(delta_table, revision_id)
    cubes = load_revision_cubes(delta_table, revision)
    populate_tree(cubes)
    elements = get_nodes_and_edges(cubes)
    return cubes, elements


def load_revision(delta_table: DeltaTable, revision_id: int) -> (dict, int):
    config = delta_table.metadata().configuration
    revision_key = f"qbeast.revision.{revision_id}"
    try:
        revision_str = config[revision_key]
        revision = json.loads(revision_str)
        return revision
    except KeyError as e:
        available_revisions = [
            k for k in config.keys() if k.startswith("qbeast.revision.")
        ]
        print(
            f"No metadata found for the given RevisionID: {revision_id}. Available revisions: {available_revisions}"
        )
        raise e


def load_revision_cubes(delta_table: DeltaTable, revision: dict) -> dict:
    revision_id_str = str(revision["revisionID"])
    dimension_count = len(revision["columnTransformers"])
    symbol_count = (dimension_count + 5) // 6
    all_cubes = dict()
    add_files = delta_table.get_add_actions(True).to_pylist()
    for add_file in add_files:
        if add_file.get("tags.revision", "-1") != revision_id_str:
            continue
        blocks = json.loads(add_file["tags.blocks"])
        for block_dict in blocks:
            cube_id = block_dict["cubeId"]
            if cube_id not in all_cubes:
                depth = len(cube_id) // symbol_count
                cube = Cube(cube_id, depth=depth)
                all_cubes[cube_id] = cube
            cube = all_cubes[cube_id]
            block = Block.from_block_and_file(block_dict, add_file)
            cube.add(block)
    return all_cubes


def populate_tree(all_cubes: dict) -> None:
    max_level = 0
    level_cubes = defaultdict(list)
    for cube in all_cubes.values():
        level_cubes[cube.depth].append(cube)
        max_level = max(max_level, cube.depth)
    for level in range(max_level):
        for cube in level_cubes[level]:
            for child in level_cubes[level + 1]:
                child.link(cube)
    return None


def get_nodes_and_edges(all_cubes: dict, fraction: float = -1.0) -> list[dict]:
    nodes = []
    edges = []
    sampling_info = SamplingInfo(fraction)
    for cube in all_cubes.values():
        node, connections = get_node_and_edges_from_cube(cube, fraction)
        nodes.append(node)
        edges.extend(connections)
        if 0.0 < fraction <= 1.0:
            sampling_info.update(cube)
    if 0.0 < fraction <= 1.0:
        print(sampling_info)
    return nodes + edges


def get_node_and_edges_from_cube(cube: Cube, fraction: float) -> (dict, list[dict]):
    selected = cube.is_sampled(fraction)
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
