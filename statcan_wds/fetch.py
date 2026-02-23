import pandas as pd
from datetime import date
import re
import logging
from typing import Optional
from urllib.parse import urlencode
from functools import lru_cache  # Added for caching

from .client import get, post 
from .resolver import build_coordinates, resolve_vectors
from .metadata import get_cube_metatdata
from .errors import VectorDataPointError

logger = logging.getLogger(__name__)

_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# --- Caching Wrapper ---

@lru_cache(maxsize=32)
def _get_cached_metadata(pid: str):
    """
    Fetch and cache cube metadata. 
    The cache persists for the lifetime of the python process.
    """
    logger.debug("Fetching fresh metadata for PID: %s", pid)
    return get_cube_metatdata(pid)

# --- Internal Helpers ---

def _validate_ref_date(value: Optional[str], field: str) -> None:
    if value is None:
        return
    if not _ISO_DATE.match(value):
        raise ValueError(f"{field} must be ISO 8601 YYYY-MM-DD, got: {value!r}")

def _coordinates_to_index(dim_map, coordinate):
    """Maps a WDS coordinate string (1.1.2.0...) to human-readable dimension names."""
    coor_arr = coordinate.split(".")
    indexes = []

    # precompute inverse maps once per call
    inv = {
        dim_name: {str(v): k for k, v in info["values"].items()}
        for dim_name, info in dim_map.items()
    }

    for dim_name, info in dim_map.items():
        pos = int(info["position"])
        if pos > len(coor_arr):
            continue

        coor_val = coor_arr[pos - 1]
        if coor_val == "0":
            continue

        label = inv[dim_name].get(coor_val)
        if label:
            indexes.append((dim_name, label))

    return dict(indexes)

def _parse_wds_response(data, dim_map):
    """Standardizes the conversion of WDS JSON responses into a list of row dicts."""
    rows = []
    for series in data:
        if series["status"] == "FAILED":
            err_obj = series.get("object", {})
            msg = (f"Failed to retrieve data point for coordinate: {err_obj.get('coordinate')}"
                   f"(Code: {err_obj.get('responseStatusCode')})")
            logger.info(msg)
            continue
        
        obj = series["object"]
        # Use the coordinate provided in the response object
        index = _coordinates_to_index(dim_map, obj["coordinate"])

        for pt in obj["vectorDataPoint"]:
            rows.append(index | {"REF_DATE": pt["refPer"], "VALUE": pt["value"]})
    
    return pd.DataFrame(rows)

# --- Specialized Fetchers ---

def _fetch_snapshot(pid, coordinates, dim_map):
    payload = [{"productId": pid, "coordinate": c, "latestN": 1} for c in coordinates]
    
    data = post("getDataFromCubePidCoordAndLatestNPeriods", payload)
    
    return _parse_wds_response(data, dim_map)

def _fetch_time_series(pid, coordinates, dim_map, metadata, ref_start, ref_end):
    vectors = resolve_vectors(pid, coordinates)

    start = ref_start or metadata.get("cubeStartDate")
    end = ref_end or metadata.get("cubeEndDate")

    params = urlencode({
        "vectorIds": ",".join(str(v) for v in vectors.keys()),
        "startRefPeriod": start,
        "endReferencePeriod": end,
    })

    data = get(f"getDataFromVectorByReferencePeriodRange?{params}")
    
    return _parse_wds_response(data, dim_map)

# --- Public API ---

def get_table_data(pid, query_spec: dict, ref_start: Optional[str] = None, ref_end: Optional[str] = None):
    """
    The main entry point. Automatically chooses between Vector (Time Series) 
    and Cube (Snapshot) endpoints based on the Product ID.
    """
    _validate_ref_date(ref_start, "ref_start")
    _validate_ref_date(ref_end, "ref_end")
    
    # Use the cached version instead of calling get_cube_metatdata directly
    metadata = _get_cached_metadata(str(pid))
    coordinates, dim_map = build_coordinates(pid, query_spec, metadata)
    
    pid_str = str(pid)

    try:
        df = _fetch_time_series(pid, coordinates, dim_map, metadata, ref_start, ref_end)
    except Exception as e:
        # Fallback for non-Census tables that might not have vectors for specific slices
        logger.info("Vector fetch failed (%s). Falling back to snapshot.", e)
        df = _fetch_snapshot(pid, coordinates, dim_map)

    if not isinstance(df, pd.DataFrame):
        raise TypeError(
            f"Internal error: expected DataFrame, got {type(df).__name__}"
        )

    return df
