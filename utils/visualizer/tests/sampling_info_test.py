import unittest

from qviz.block import File, Block
from qviz.cube import Cube
from qviz.sampling_info import SamplingInfoBuilder


class TestSamplingInfo(unittest.TestCase):
    def setUp(self):
        self.file = File(path="test_path", bytes=1000, num_rows=10)
        self.block = Block(
            cube_id="",
            element_count=10,
            min_weight=-2147483648,  # maps to 0.0 in normalize_weight
            max_weight=0,  # maps to 0.5 in normalize_weight
            file=self.file,
        )
        self.cube = Cube(cube_id="", depth=0)
        self.cube.add(self.block)

        self.another_file = File(path="another_test_path", bytes=2000, num_rows=20)
        self.another_block = Block(
            cube_id="A",
            element_count=20,
            min_weight=0,  # maps to 0.5 in normalize_weight
            max_weight=2147483647,  # maps to 1.0 in normalize_weight
            file=self.another_file,
        )
        self.another_cube = Cube(cube_id="B", depth=1)
        self.another_cube.add(self.another_block)

        self.sampling_info_builder = SamplingInfoBuilder(f=0.4)

    def test_update(self):
        self.sampling_info_builder.update(self.cube)
        sampling_info = self.sampling_info_builder.result()
        self.assertEqual(sampling_info.total_rows, 10)
        self.assertEqual(sampling_info.total_bytes, 1000)
        self.assertEqual(sampling_info.sampled_rows, 10)
        self.assertEqual(sampling_info.sampled_bytes, 1000)

        self.sampling_info_builder.update(self.another_cube)
        sampling_info = self.sampling_info_builder.result()
        self.assertEqual(sampling_info.total_rows, 30)
        self.assertEqual(sampling_info.total_bytes, 3000)
        self.assertEqual(sampling_info.sampled_rows, 10)
        self.assertEqual(sampling_info.sampled_bytes, 1000)
