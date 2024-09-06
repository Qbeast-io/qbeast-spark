from collections import defaultdict
from qviz.cube import Cube, SamplingInfo


def process_add_files(add_files: list[dict], metadata: dict) -> list[Cube]:
    """
    Convert AddFiles to Cubes
    :param add_files:  a list of AddFile dictionaries
    :param metadata: metadata configuration for the target revision
    :return: a list of all cube from the given revision
    """
    if metadata:
        dimension_count = len(metadata['columnTransformers'])
        symbol_count = (dimension_count + 5) // 6
    else:
        print(f"No metadata found for the given RevisionID.")
        symbol_count = float('inf')
        for add_file in add_files:
            cube_encoding_size = len(add_file['tags']['cube'])
            if 0 < cube_encoding_size < symbol_count:
                symbol_count = cube_encoding_size

    # Group cube blocks by Cube string
    cube_blocks = defaultdict(list)
    for add_file in add_files:
        cube_string = add_file['tags']['cube']
        cube_blocks[cube_string].append(add_file)

    cubes = []
    for blocks in cube_blocks.values():
        cube_string = blocks[0]['block']
        depth = len(cube_string) // symbol_count

        size = 0
        max_weight = float("inf")
        element_count = 0
        for add_file in blocks:
            tags = add_file['tags']

            max_weight = min(max_weight, int(tags['maxWeight']))
            element_count += int(tags['elementCount'])
            size += int(add_file['size'])

        cubes.append(Cube(cube_string, max_weight, element_count, size, depth))

    return cubes


def populate_tree(cubes: list[Cube]) -> Cube:
    """
    Populate tree by iterating cubes from top down and creating parent-child references
    :param cubes: list of Cube
    :return: root cube with reference to child cubes
    """
    max_level = 0
    level_cubes = defaultdict(list)
    for cube in cubes:
        level_cubes[cube.depth].append(cube)
        max_level = max(max_level, cube.depth)

    for level in range(max_level):
        for cube in level_cubes[level]:
            for child in level_cubes[level + 1]:
                child.link(cube)
                
    root = level_cubes[0][0]
    return root


def get_nodes_and_edges(cubes: list[Cube], fraction: float = -1.0) -> list[dict]:
    """
    Compute nodes and edges to draw according to a given sampling fraction,
    :param cubes: list of populated cubes
    :param fraction: sampling fraction from 0 to 1
    :return: nodes and edges to draw
    """
    nodes = []
    edges = []

    sampling_info = SamplingInfo(fraction)
    for cube in cubes:
        node, connections = cube.get_elements_for_sampling(fraction)
        nodes.append(node)
        edges.extend(connections)

        sampling_info.update(cube, node['selected'])

    if fraction > 0:
        print(sampling_info)

    return nodes + edges
