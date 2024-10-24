from unittest import TestCase

from qviz.block import Block, File


class BlockTest(TestCase):
    def setUp(self):
        self.SCALA_MIN = -2147483648  # maps to 0.0 in normalize_weight
        self.SCALA_MAX = 2147483647  # maps to 1.0 in normalize_weight
        self.block = Block(
            cube_id="A",
            element_count=10,
            min_weight=-1073741824,  # maps to 0.25 in normalize_weight
            max_weight=1073741823,  # maps to 0.75 in normalize_weight
            file=File(path="test_path", bytes=1024, num_rows=100),
        )

    def test_from_block_and_file(self):
        block_json = {
            "cubeId": "",
            "elementCount": 10,
            "minWeight": self.SCALA_MIN,
            "maxWeight": self.SCALA_MAX,
        }
        add_file_json = {"path": "file_1", "size_bytes": 1024, "num_records": 100}
        block = Block.from_block_and_file(block_json, add_file_json)

        self.assertEqual(block.cube_id, "")
        self.assertEqual(block.element_count, 10)
        self.assertEqual(block.min_weight, 0.0)
        self.assertEqual(block.max_weight, 1.0)
        self.assertEqual(block.file.path, "file_1")
        self.assertEqual(block.file.bytes, 1024)
        self.assertEqual(block.file.num_rows, 100)

    def test_normalize_weight(self):
        self.assertEqual(Block.normalize_weight(self.SCALA_MIN), 0.0)
        self.assertEqual(Block.normalize_weight(self.SCALA_MAX), 1.0)
        self.assertEqual(Block.normalize_weight(0), 0.5)
        self.assertEqual(Block.normalize_weight(-1073741824), 0.25)
        self.assertEqual(Block.normalize_weight(1073741823), 0.75)

    def test_is_sampled(self):
        self.assertFalse(self.block.is_sampled(0.24))
        self.assertTrue(self.block.is_sampled(0.25))
        self.assertTrue(self.block.is_sampled(0.5))
        self.assertTrue(self.block.is_sampled(0.9))
