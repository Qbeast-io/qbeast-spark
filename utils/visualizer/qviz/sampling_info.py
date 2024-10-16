from qviz.block import Block
from qviz.cube import Cube


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
