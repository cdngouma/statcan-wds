import pandas as pd
from datetime import date
from typing import Optional
import re
from .client import get, post 
from .resolver import build_coordinates, resolve_vectors
from .metadata import get_cube_metatdata
from .errors import VectorDataPointError

_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# --- Internal Helpers ---

def coordinates_to_index(dim_map, coordinate):
    """Maps a WDS coordinate string (1.1.2.0...) to human-readable dimension names."""
    coor_arr = coordinate.split(".")
    indexes = []

    for dim_name, info in dim_map.items():
        pos = int(info["position"])
        vals = info["values"]
        
        # Ensure the coordinate array is long enough for this dimension
        if pos > len(coor_arr):
            continue
            
        coor_val = coor_arr[pos - 1]
        
        # Skip dimensions with '0' (not specified in this specific slice)
        if coor_val == "0":
            continue
        
        # Find the member name that matches the ID at this position
        idx_val = next((name for name, mId in vals.items() if int(mId) == int(coor_val)), None)
        
        if idx_val:
            indexes.append((dim_name, idx_val))
    
    return dict(indexes)

def _parse_wds_response(data, dim_map):
    """Standardizes the conversion of WDS JSON responses into a list of row dicts."""
    rows = []
    for series in data:
        if series["status"] == "FAILED":
            err_obj = series.get("object", {})
            msg = f"API Error: {series['status']} (Code: {err_obj.get('responseStatusCode', 'Unknown')})"
            raise VectorDataPointError(msg)
        
        obj = series["object"]
        # Use the coordinate provided in the response object
        index = coordinates_to_index(dim_map, obj["coordinate"])

        for pt in obj["vectorDataPoint"]:
            rows.append(index | {"REF_DATE": pt["refPer"], "VALUE": pt["value"]})
    return rows

# --- Specialized Fetchers ---

def _fetch_snapshot(pid, coordinates, dim_map):
    """Internal: Uses the Cube-Coordinate endpoint."""
    payload = [{"productId": pid, "coordinate": c, "latestN": 1} for c in coordinates]
    data = post("getDataFromCubePidCoordAndLatestNPeriods", payload)
    return _parse_wds_response(data, dim_map)

def _fetch_time_series(pid, coordinates, dim_map, metadata, ref_start, ref_end):
    """Internal: Uses the Vector-Range endpoint."""
    vectors = resolve_vectors(pid, coordinates)
    
    vector_ids = ",".join(f'"{v}"' for v in vectors.keys())
    start = ref_start or metadata.get("cubeStartDate")
    end = ref_end or metadata.get("cubeEndDate")

    query = (
        f"getDataFromVectorByReferencePeriodRange"
        f"?vectorIds={vector_ids}&startRefPeriod={start}&endReferencePeriod={end}"
    )
    
    data = get(query)
    return _parse_wds_response(data, dim_map)

# --- Public API ---

def get_table_data(pid, query_spec: dict, ref_start: Optional[str] = None, ref_end: Optional[str] = None):
    """
    The main entry point. Automatically chooses between Vector (Time Series) 
    and Cube (Snapshot) endpoints based on the Product ID.
    """
    metadata = get_cube_metatdata(pid)
    coordinates, dim_map = build_coordinates(pid, query_spec, metadata)
    
    pid_str = str(pid)

    try:
        # Route based on PID (Census tables start with 98)
        if pid_str.startswith("98"):
            results = _fetch_snapshot(pid, coordinates, dim_map)
        else:
            results = _fetch_time_series(pid, coordinates, dim_map, metadata, ref_start, ref_end)
    except Exception as e:
        # Fallback for non-Census tables that might not have vectors for specific slices
        print(f"Primary fetch method failed ({e}), attempting snapshot fallback...")
        results = _fetch_snapshot(pid, coordinates, dim_map)

    return pd.DataFrame(results)