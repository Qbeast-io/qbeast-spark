from __future__ import annotations

OFFSET = -2147483648.0  # Scala Int.MinValue
RANGE = 2147483647.0 - OFFSET


class Block:
    def __init__(self, cube_id: str, element_count: int, min_weight: int, max_weight: int, file_path: str, file_bytes: int) -> None:
        self.cube_id = cube_id
        self.element_count = element_count
        self.min_weight = self.normalize_weight(min_weight)
        self.max_weight = self.normalize_weight(max_weight)
        self.file_path = file_path
        self.file_bytes = file_bytes

    @staticmethod
    def normalize_weight(weight: int) -> float:
        """
        Map Weight to NormalizedWeight
        :param weight: Weight
        :return: A Weight's corresponding NormalizedWeight
        """
        fraction = (weight - OFFSET) / RANGE
        # We make sure fraction is within [0, 1]
        normalized = max(0.0, min(1.0, fraction))
        return float("{:.3f}".format(normalized))

    @classmethod
    def from_dict(cls, json_dict: dict, file_path: str, file_bytes: int) -> Block:
        cube_id = json_dict["cubeId"]
        element_count = json_dict["elementCount"]
        min_weight = json_dict["minWeight"]
        max_weight = json_dict["maxWeight"]
        return cls(
            cube_id=cube_id,
            element_count=element_count,
            min_weight=min_weight,
            max_weight=max_weight,
            file_path=file_path,
            file_bytes=file_bytes
        )

    def is_sampled(self, fraction: float) -> bool:
        """
        Determine if the cube is to be included in sampling for a given fraction
        :param fraction: sampling fraction between 0 and 1
        :return: boolean determining if the cube is selected
        """
        assert 0 < fraction <= 1, f"Invalid fraction value: {fraction}, it must be between 0 and 1."
        return self.min_weight <= fraction


class Cube:
    def __init__(
        self,
        cube_id: str,
        depth: int,
    ):
        self.cube_id = cube_id
        self.depth = depth

        self.max_weight = float("inf")
        self.min_weight = float("inf")
        self.element_count = 0
        self.size = 0
        self.children = []
        self.blocks = []
        self.parent = None

    def add(self, block: Block) -> None:
        self.blocks.append(block)
        self.max_weight = min(self.max_weight, block.max_weight)
        self.min_weight = min(self.min_weight, block.min_weight)
        self.element_count += block.element_count
        self.size += block.file_bytes

    def link(self, that: Cube) -> None:
        """
        Establish parent-child cube relationship for two cubes if appropriate
        :param that: potential parent cube
        :return: None
        """
        if self.is_child_of(that):
            self.parent = that
            that.children.append(self)

    def is_child_of(self, that: Cube) -> bool:
        return (
            not self.is_root()
            and that.depth + 1 == self.depth
            and self.cube_id.startswith(that.cube_id)
        )

    def is_root(self) -> bool:
        return self.depth == 0

    def __repr__(self) -> str:
        return f"Cube: {self.cube_id}, count: {self.element_count}, maxWeight: {self.max_weight}"


class SamplingInfo:
    def __init__(self, f: float) -> None:
        self.fraction = f
        self.total_cubes = 0
        self.sampled_cubes = 0
        self.total_rows = 0
        self.sampled_rows = 0
        self.total_size = 0
        self.sampled_size = 0

    def update(self, cube: Cube, is_sampled: bool) -> None:
        self.total_cubes += 1
        self.total_rows += cube.element_count
        self.total_size += cube.size / (1024 * 1024)

        if is_sampled:
            self.sampled_cubes += 1
            self.sampled_rows += cube.element_count
            self.sampled_size += cube.size / (1024 * 1024)

    def __repr__(self) -> str:
        disclaimer = """Disclaimer:
        \tThe displayed sampling metrics are valid only for single revision indexes(excluding revision 0):"""
        file_count_percentage = self.sampled_cubes / self.total_cubes * 100
        row_count_percentage = self.sampled_rows / self.total_rows * 100
        size_count_percentage = self.sampled_size / self.total_size * 100
        return """Sampling Info:\
        \n\t{}
        \n\tsample fraction: {}\
        \n\tnumber of cubes read:{}/{}, {:.2f}%\
        \n\tnumber of rows: {}/{}, {:.2f}%\
        \n\tsample size: {:.5f}/{:.5f}GB, {:.2f}%""".format(
            disclaimer,
            self.fraction,
            self.sampled_cubes,
            self.total_cubes,
            file_count_percentage,
            self.sampled_rows,
            self.total_rows,
            row_count_percentage,
            self.sampled_size / 1024,
            self.total_size / 1024,
            size_count_percentage,
        )
