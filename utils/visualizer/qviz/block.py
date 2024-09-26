from qviz.cube import normalize_weight

class Block:
    def __init__(
        self,
        cube_id: str,
        element_count: int, 
        min_weight: int,
        max_weight: int,
        file: str,
        size: int,
    ):
        self.cube_id = cube_id
        self.file = file
        self.min_weight = normalize_weight(min_weight)
        self.max_weight = normalize_weight(max_weight)
        self.element_count = element_count
        self.size = size
        self.children = []
        self.parent = None