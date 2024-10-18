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

    def __hash__(self) -> int:
        return hash(self.path)


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
