from itertools import product
from .client import post
from .metadata import inspect_dimensions
from .errors import (
    InvalidDimensionError,
    InvalidMemberError,
    InvalidCoordinateError
)


def expand_specs(query_spec):
    """
    Expand a compact spec into the cartesian product of choices.
    Example:
        input: [
            { "Geography": ["Quebec", "Canada"] },
            { "Trade": ["Import", "Domestic-exports"] },
            { "NAPCS": "All merchandise" }
        ]

        output: [
            { "Geography": "Quebec", "Trade":" Import", "NAPCS": "All merchandise" },
            ...
        ]
    """
    pairs = []
    for dim in query_spec:
        (k,v), = dim.items()
        vals = v if isinstance(v, (list, tuple)) else [v]
        pairs.append((k, list(vals)))
    
    keys = [k for k,_ in pairs]
    value_lists = [vals for _, vals in pairs]

    return [
        dict(zip(keys, combo))
        for combo in product(*value_lists)
    ]


def build_coordinates(pid, expanded_specs):
    """
    Map human-readable specs to WDS coordinate strings
    """
    dims = inspect_dimensions(pid)

    coordinates = []
    for series in expanded_specs:
        slots = ["0"] * 10 # Coordinates have up to 10 slots

        for dim_name, member_name in series.items():
            dim = dims.get(dim_name)
            if dim is None:
                raise InvalidDimensionError(f"Dimension '{dim_name}' not found in cube {pid}")
            
            member_id = dim["values"].get(member_name)
            if member_id is None:
                raise InvalidMemberError(f"Member '{member_name}' not found in dimension '{dim_name}'")
            
            slots[int(dim["position"]) - 1] = str(member_id)
        
        coordinates.append(".".join(slots))
    
    dim_map = {k: int(v["position"]) for k,v in dims.items()}

    return coordinates, dim_map


def resolve_vectors(pid, coordinates):
    """
    Resolve WDS coordinates to vectors IDs
    """
    if not coordinates:
        raise InvalidCoordinateError("No coordinates provided")
    
    payload = [
        {"productId": pid, "coordinate": c}
        for c in coordinates
    ]

    series = post("getSeriesInfoFromCubePidCoord", payload)

    vec_map = {
        s["object"]["vectorId"]: s["object"]["SeriesTitleEn"]
        for s in series
        if s["object"]["vectorId"] != 0
    }

    if not vec_map:
        raise InvalidCoordinateError(f"Invalid coordinate combinations: {coordinates}")
    
    return vec_map
