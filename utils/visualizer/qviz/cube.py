from __future__ import annotations
from dataclasses import dataclass

OFFSET = -2147483648.0  # Scala Int.MinValue
RANGE = 2147483647.0 - OFFSET


@dataclass
class File:
    path: str
    bytes: int
    num_rows: int

    @classmethod
    def from_add_file_json(cls, add_file: dict) -> File:
        _path = add_file["path"]
        _bytes = add_file["size_bytes"]
        _num_rows = add_file["num_records"]
        return cls(_path, _bytes, _num_rows)


class Block:
    def __init__(
        self,
        cube_id: str,
        element_count: int,
        min_weight: int,
        max_weight: int,
        file: File,
    ) -> None:
        self.cube_id = cube_id
        self.element_count = element_count
        self.min_weight = self.normalize_weight(min_weight)
        self.max_weight = self.normalize_weight(max_weight)
        self.file = file

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
    def from_block_and_file(cls, block_json: dict, add_file_json: dict) -> Block:
        cube_id = block_json["cubeId"]
        element_count = block_json["elementCount"]
        min_weight = block_json["minWeight"]
        max_weight = block_json["maxWeight"]
        file = File.from_add_file_json(add_file_json)
        return cls(
            cube_id=cube_id,
            element_count=element_count,
            min_weight=min_weight,
            max_weight=max_weight,
            file=file,
        )

    def is_sampled(self, fraction: float) -> bool:
        """
        Determine if the cube is to be included in sampling for a given fraction
        :param fraction: sampling fraction between 0 and 1
        :return: boolean determining if the cube is selected
        """
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
        self.size += block.file.bytes

    def is_sampled(self, fraction: float) -> bool:
        return any(block.is_sampled(fraction) for block in self.blocks)

    def link(self, that: Cube) -> None:
        """
        Establish parent-child cube relationship for two cubes if appropriate
        :param that: potential parent cube
        :return: None
        """
        if self._is_child_of(that):
            self.parent = that
            that.children.append(self)

    def _is_child_of(self, that: Cube) -> bool:
        return (
            self.depth > 0
            and self.depth == that.depth + 1
            and self.cube_id.startswith(that.cube_id)
        )

    def __repr__(self) -> str:
        return f"Cube: {self.cube_id}, count: {self.element_count}, maxWeight: {self.max_weight}"


class SamplingInfo:
    def __init__(self, f: float) -> None:
        self.fraction = f
        self.total_rows = 0
        self.sampled_rows = 0
        self.total_bytes = 0
        self.sampled_bytes = 0
        self.selected_files = set()

    def update(self, cube: Cube) -> None:
        self.total_rows += cube.element_count
        self.total_bytes += cube.size
        for block in cube.blocks:
            self._update(block)

    def _update(self, block: Block) -> None:
        if (
            block.is_sampled(self.fraction)
            and block.file.path not in self.selected_files
        ):
            self.sampled_rows += block.file.num_rows
            self.sampled_bytes += block.file.bytes
            self.selected_files.add(block.file.path)

    def __repr__(self) -> str:
        disclaimer = """Disclaimer:
        \tThe displayed sampling metrics are only for the chosen revisionId.
        \tThe values will be different if the table contains multiple revisions."""
        num_rows_percentage = (
            self.sampled_rows / self.total_rows * 100 if (self.total_rows > 0) else -1
        )
        sampled_bytes_percentage = (
            self.sampled_bytes / self.total_bytes * 100 if (self.total_rows > 0) else -1
        )
        return """Sampling Info:\
        \n\t{}
        \n\tsample fraction: {}\
        \n\tnumber of rows: {}/{}, {:.2f}%\
        \n\tsample size: {:.5f}/{:.5f}GB, {:.2f}%""".format(
            disclaimer,
            self.fraction,
            self.sampled_rows,
            self.total_rows,
            num_rows_percentage,
            self.sampled_bytes / 1e9,
            self.total_bytes / 1e9,
            sampled_bytes_percentage,
        )
