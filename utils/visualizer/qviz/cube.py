from __future__ import annotations

OFFSET = -2147483648.0  # Scala Int.MinValue
RANGE = 2147483647.0 - OFFSET


class Cube:
    def __init__(
        self,
        cube_string: str,
        max_weight: int,
        element_count: int,
        size: int,
        depth: int,
    ):
        self.cube_string = cube_string
        self.max_weight = normalize_weight(max_weight)
        self.element_count = element_count
        self.depth = depth
        self.size = size
        self.children = []
        self.blocks = []
        self.parent = None

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
            self.is_not_root()
            and that.depth + 1 == self.depth
            and self.cube_string.startswith(that.cube_string)
        )

    def is_not_root(self) -> bool:
        return bool(self.cube_string)

    def get_elements_for_sampling(self, fraction: float) -> (dict, list[dict]):
        """
        Return cube and edges for drawing if the cube or any of its children are selected
        for the given sampling fraction.
        :param fraction: sampling fraction between [0, 1]
        :return: styled node and edges for drawing depending on if they are selected for
        sampling
        """
        selected = self.is_sampled(fraction)
        name = self.cube_string or "root"
        label = (name + " " if name == "root" else "") + str(self.max_weight)
        node = {
            "data": {"id": name, "label": label},
            "selected": selected,
            "classes": "sampled" if selected else "",
        }

        edges = []
        for child in self.children:
            selected_child = child.is_sampled(fraction)
            edges.append(
                {
                    "data": {"source": name, "target": child.cube_string},
                    "selected": selected_child,
                    "classes": "sampled" if selected_child else "",
                }
            )

        return node, edges

    def is_sampled(self, fraction: float) -> bool:
        """
        Determine if the cube is to be included in sampling for a given fraction
        :param fraction: sampling fraction between 0 and 1
        :return: boolean determining if the cube is selected
        """
        return fraction > 0 and (
            self.parent is None or self.parent.max_weight < fraction
        )

    def __repr__(self) -> str:
        return f"Cube: {self.cube_string}, count: {self.element_count}, maxWeight: {self.max_weight}"


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


def normalize_weight(weight: int) -> float:
    """
    Map Weight to NormalizedWeight
    :param weight: Weight
    :return: weight's corresponding NormalizedWeight
    """
    fraction = (weight - OFFSET) / RANGE
    # We make sure fraction is in range [0, 1] using max & min
    normalized = max(0, min(1, fraction))

    return float("{:.3f}".format(normalized))
