import unittest
from qviz.cube import Cube, SamplingInfo


class TestCube(unittest.TestCase):
    def test_is_not_root(self):
        root = Cube("", 1, 1, 1, 0)
        cube_level_1 = Cube("A", 1, 1, 1, 1)

        self.assertFalse(root.is_not_root())
        self.assertTrue(cube_level_1.is_not_root())

    def test_link(self):
        root = Cube("", 1, 1, 1, 0)
        cube_level_1 = Cube("A", 1, 1, 1, 1)
        cube_level_2 = Cube("AA", 1, 1, 1, 2)

        cube_level_1.link(root)
        self.assertTrue(cube_level_1.parent is root)
        self.assertTrue(root.children[0] is cube_level_1)

        cube_level_2.link(root)
        self.assertTrue(cube_level_2.parent is not root)
        self.assertTrue(len(root.children) == 1)

    def test_is_sampled(self):
        root = Cube("", -1269647486, 100, 100, 0)  # max weight: 0.204
        cube = Cube("A", 100, 100, 100, 1)  # max weight: 0.5
        cube.link(root)

        self.assertTrue(root.is_sampled(0.3))
        self.assertTrue(cube.is_sampled(0.3))

        self.assertTrue(root.is_sampled(0.1))
        self.assertFalse(cube.is_sampled(0.1))

    def test_get_elements_for_sampling(self):
        root = Cube("", -1269647486, 100, 100, 0)  # max weight: 0.204
        cube = Cube("A", 10000000000, 5, 5, 1)  # max weight 2.828
        cube.link(root)

        node, edges = root.get_elements_for_sampling(0.15)
        self.assertTrue(node['data']['id'] == 'root')
        self.assertTrue(node['selected'])
        self.assertTrue(edges[0]['data']['source'] == 'root')
        self.assertTrue(edges[0]['data']['target'] == 'A')
        self.assertFalse(edges[0]['selected'])


class TestSamplingInfo(unittest.TestCase):
    def test_update(self):
        root = Cube("", -1269647486, 100, 5, 0)  # max weight: 0.204
        cube_level_1 = Cube("A", 100, 100, 5, 1)  # max weight: 0.5
        cube_level_2 = Cube("A", 10000000000, 50, 1, 2)  # max weight 2.828

        cube_level_1.link(root)
        cube_level_2.link(cube_level_1)

        f = 0.3
        sampling_info = SamplingInfo(f)
        sampling_info.update(root, root.is_sampled(f))
        sampling_info.update(cube_level_1, cube_level_1.is_sampled(f))
        sampling_info.update(cube_level_2, cube_level_2.is_sampled(f))

        self.assertTrue(sampling_info.total_cubes == 3)
        self.assertTrue(sampling_info.sampled_cubes == 2)

        self.assertTrue(sampling_info.total_rows == 250)
        self.assertTrue(sampling_info.sampled_rows == 200)

        self.assertTrue(sampling_info.total_size == 11 / (1024 * 1024))
        self.assertTrue(sampling_info.sampled_size == 10 / (1024 * 1024))
