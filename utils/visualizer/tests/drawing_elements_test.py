import unittest
from qviz.drawing_elements import process_add_files, get_nodes_and_edges, populate_tree


class TestDrawingElementPreparation(unittest.TestCase):
    def setUp(self) -> None:
        self.add_files = [
            {
                "size": 1,
                "tags": {
                    "state": "FLOODED",
                    "cube": "",
                    "elementCount": "100",
                    "maxWeight": "-1717986918",
                },
            },
            {
                "size": 1,
                "tags": {
                    "state": "FLOODED",
                    "cube": "A",
                    "elementCount": "50",
                    "maxWeight": "2147483647",
                },
            },
            {
                "size": 1,
                "tags": {
                    "state": "FLOODED",
                    "cube": "g",
                    "elementCount": "100",
                    "maxWeight": "-1269647486",
                },
            },
            {
                "size": 1,
                "tags": {
                    "state": "FLOODED",
                    "cube": "g",
                    "elementCount": "100",
                    "maxWeight": "-1269647486",
                },
            },
        ]
        self.metadata = {
            "desiredCubeSize": 10000,
            "columnTransformers": [
                {
                    "className": "io.qbeast.core.transform.LinearTransformer",
                    "columnName": "user_id",
                    "dataType": "IntegerDataType",
                },
                {
                    "className": "io.qbeast.core.transform.LinearTransformer",
                    "columnName": "price",
                    "dataType": "DoubleDataType",
                },
            ],
            "transformations": [],
        }

    def test_add_files_processing(self):
        cubes = process_add_files(self.add_files, self.metadata)

        self.assertTrue(len(cubes) == 3)
        for cube in cubes:
            if cube.cube_string == "g":
                self.assertTrue(cube.element_count == 200)

    def test_populate_tree(self):
        cubes = process_add_files(self.add_files, self.metadata)
        root = populate_tree(cubes)

        for c in root.children:
            self.assertTrue(c.parent, root)

    def test_extract_nodes_and_edges(self):
        cubes = process_add_files(self.add_files, self.metadata)

        elements = get_nodes_and_edges(cubes)

        nodes, edges = [], []
        for element in elements:
            if "source" in element["data"]:
                edges.append(element)
            else:
                nodes.append(element)

        node_names = sorted([node["data"]["id"] for node in nodes])
        cube_strings = sorted([cube.cube_string or "root" for cube in cubes])
        for node_name, cube_name in zip(node_names, cube_strings):
            self.assertTrue(node_name, cube_name)

        for edge in edges:
            if edge["data"]["source"] == "root":
                self.assertIn(edge["data"]["target"], ["A", "g"])
