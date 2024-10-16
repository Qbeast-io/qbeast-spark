import unittest

from qviz.block import File, Block
from qviz.cube import Cube
from qviz.sampling_info import SamplingInfo


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

        self.sampling_info = SamplingInfo(f=0.4)

    def test_update(self):
        self.sampling_info.update(self.cube)
        self.assertEqual(self.sampling_info.total_rows, 10)
        self.assertEqual(self.sampling_info.total_bytes, 1000)
        self.assertEqual(self.sampling_info.sampled_rows, 10)
        self.assertEqual(self.sampling_info.sampled_bytes, 1000)

        self.sampling_info.update(self.another_cube)
        self.assertEqual(self.sampling_info.total_rows, 30)
        self.assertEqual(self.sampling_info.total_bytes, 3000)
        self.assertEqual(self.sampling_info.sampled_rows, 10)
        self.assertEqual(self.sampling_info.sampled_bytes, 1000)

    def test_repr(self):
        self.sampling_info.update(self.cube)
        expected_repr = """Sampling Info:\
        \n\tDisclaimer:
        \tThe displayed sampling metrics are only for the chosen revisionId.
        \tThe values will be different if the table contains multiple revisions.\
        \n\tsample fraction: 0.4\
        \n\tnumber of rows: 100/10, 1000.00%\
        \n\tsample size: 0.00000/0.00000GB, 1000.00%"""
        self.assertIn("Sampling Info:", repr(self.sampling_info))

        self.sampling_info.update(self.another_cube)
        expected_repr = """Sampling Info:\
        \n\tDisclaimer:
        \tThe displayed sampling metrics are only for the chosen revisionId.
        \tThe values will be different if the table contains multiple revisions.\
        \n\tsample fraction: 0.4\
        \n\tnumber of rows: 200/20, 1000.00%\
        \n\tsample size: 0.00000/0.00000GB, 1000.00%"""
        self.assertIn("Sampling Info:", repr(self.sampling_info))
