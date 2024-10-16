import unittest

from qviz.block import Block, File
from qviz.cube import Cube


class TestCube(unittest.TestCase):
    def setUp(self):
        self.file = File(path="test_path", bytes=1000, num_rows=10)
        self.block = Block(
            cube_id="A",
            element_count=5,
            min_weight=-1073741824,  # maps to 0.25 in normalize_weight
            max_weight=1073741823,  # maps to 0.75 in normalize_weight
            file=self.file,
        )
        self.cube = Cube(cube_id="A", depth=1)

    def test_add_block(self):
        self.cube.add(self.block)
        self.assertEqual(len(self.cube.blocks), 1)
        self.assertEqual(self.cube.element_count, 5)
        self.assertEqual(self.cube.size, 1000)
        self.assertEqual(self.cube.min_weight, 0.25)
        self.assertEqual(self.cube.max_weight, 0.75)

        # Add another block
        another_block = Block(
            cube_id="A",
            element_count=10,
            min_weight=-2147483648,  # maps to 0.0 in normalize_weight
            max_weight=0,  # maps to 1.0 in normalize_weight
            file=File(path="another_test_path", bytes=2000, num_rows=20),
        )
        self.cube.add(another_block)
        self.assertEqual(len(self.cube.blocks), 2)
        self.assertEqual(self.cube.element_count, 15)
        self.assertEqual(self.cube.size, 3000)
        self.assertEqual(self.cube.min_weight, 0.0)
        self.assertEqual(self.cube.max_weight, 0.5)

    def test_is_sampled(self):
        self.cube.add(self.block)
        self.assertFalse(self.cube.is_sampled(0.1))
        self.assertTrue(self.cube.is_sampled(0.3))

    def test_link(self):
        parent_cube = Cube(cube_id="", depth=0)
        self.cube.link(parent_cube)
        self.assertEqual(self.cube.parent, parent_cube)
        self.assertIn(self.cube, parent_cube.children)
