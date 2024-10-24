from qviz.block import File
from qviz.cube import Cube
from dataclasses import dataclass


@dataclass
class SamplingInfo:
    fraction: float
    total_rows: int
    total_bytes: int
    sampled_rows: int
    sampled_bytes: int

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
        return f"""Sampling Info:\
        \n\t{disclaimer}
        \n\tsample fraction: {self.fraction}\
        \n\tsampled rows: {self.sampled_rows}/{self.total_rows}, {num_rows_percentage:.2f}%\
        \n\tsampled size: {self.sampled_bytes / 1e9:.5f}/{self.total_bytes / 1e9:.5f}GB, {sampled_bytes_percentage:.2f}%"""


class SamplingInfoBuilder:
    def __init__(self, f: float) -> None:
        self.fraction: float = f
        self.sampled_files: set[File] = set()
        self.all_files: set[File] = set()

    def update(self, cube: Cube) -> None:
        for block in cube.blocks:
            self.all_files.add(block.file)
            if block.is_sampled(self.fraction):
                self.sampled_files.add(block.file)

    def result(self) -> SamplingInfo:
        total_rows = sum(f.num_rows for f in self.all_files)
        total_bytes = sum(f.bytes for f in self.all_files)
        sampled_rows = sum(f.num_rows for f in self.sampled_files)
        sampled_bytes = sum(f.bytes for f in self.sampled_files)
        return SamplingInfo(
            self.fraction,
            total_rows,
            total_bytes,
            sampled_rows,
            sampled_bytes,
        )
