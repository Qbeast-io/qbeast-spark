from __future__ import annotations

from qviz.block import Block


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
