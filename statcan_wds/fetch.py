import pandas as pd
from datetime import date
from typing import Optional
import re
from .client import get
from .resolver import build_coordinates, resolve_vectors
from .metadata import get_cube_metatdata
from .errors import VectorDataPointError


_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _normalize_ref_period_range(ref_start: Optional[str], ref_end: Optional[str]):
    if ref_start is None and ref_end is None:
        raise ValueError("Both start_ref_period and end_ref_period are None. Provide at least one.")
    
    if ref_start is None:
        ref_start = ref_end
    
    if ref_end is None:
        ref_end = date.today().isoformat()

    assert ref_start is not None and ref_end is not None
    
    if not _ISO_DATE.match(ref_start):
        raise ValueError(f"start_ref_period must be YYYY-MM-DD, got: {ref_start}")
    
    if not _ISO_DATE.match(ref_end):
        raise ValueError(f"end_ref_period must be YYYY-MM-DD, got: {ref_end}")

    if ref_start > ref_end:
        raise ValueError(f"start_ref_period ({ref_start}) is after end_ref_period ({ref_end})")

    return ref_start, ref_end


def get_table_data(
    pid, 
    query_spec: dict, 
    start_ref_period: Optional[str] = None,
    end_ref_period: Optional[str] = None
):
    """
    Fetch multiple StatCan series into a tidy DataFrame
    """

    start_ref_period, end_ref_period = _normalize_ref_period_range(
        start_ref_period, 
        end_ref_period
    )

    metadata = get_cube_metatdata(pid)
    coordinates, dim_map = build_coordinates(pid, query_spec, metadata)
    vectors = resolve_vectors(pid, coordinates)

    vector_ids = ",".join(f'"{v}"' for v in vectors.keys())

    query = (
        f"getDataFromVectorByReferencePeriodRange"
        f"?vectorIds={vector_ids}"
        f"&startRefPeriod={start_ref_period}"
        f"&endReferencePeriod={end_ref_period}"
    )

    data = get(query)

    print("vectors::", vectors)

    print("data::", data)

    print("dim_map::", dim_map)

    final_df = []

    for series in data:
        if series["status"] == "FAILED":
            raise VectorDataPointError(
                "Failed to retrieve vector data points: "
                f"responseStatusCode: {series['object']['responseStatusCode']}"
            )
        
        obj = series["object"]
        v_id = obj["vectorId"]

        #index_cols = [list(d.keys())[0] for d in query_spec]
        #index_cols.sort(key=lambda c: dim_map.get(c, float("inf")))
        index_cols = list(dim_map.keys())
        index_cols.sort(key=lambda c: dim_map.get(c, float("inf")))

        index_vals = vectors[v_id].split(";")
        index = dict(zip(index_cols, index_vals))

        print("---", index)

        for pt in obj["vectorDataPoint"]:
            final_df.append(
                index
                | {
                    "REF_DATE": pt["refPer"], 
                    "VALUE": pt["value"]
                }
            )

    return pd.DataFrame(final_df)
