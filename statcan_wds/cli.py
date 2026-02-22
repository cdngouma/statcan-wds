from typing import Optional, Any
import argparse
import json
from pathlib import Path
import yaml

from .metadata import inspect_dimensions, get_cube_metatdata
from .fetch import get_table_data


def _load_config(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise SystemExit(f"Config file not found: {path}")
    
    suffix = p.suffix.lower()
    if suffix in (".yaml", ".yml"):
        with p.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    elif suffix == ".json":
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    
    raise SystemExit("Config must be .yaml/.yml or .json")


def _fetch_data(args, cfg: dict[str, Any]) -> None:
    pid = cfg.get("pid")
    if not pid:
        raise SystemExit("Config must include 'pid'.")
    
    query_spec = cfg.get("query", None)
    if query_spec is None:
        SystemExit("Config must include 'query'.")
    
    start = args.start if args.start is not None else cfg.get("ref_start")
    end = args.end if args.end is not None else cfg.get("ref_end")

    df = get_table_data(
        pid=pid,
        query_spec=query_spec,
        ref_start=start,
        ref_end=end,
    )

    # Output handling
    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        fmt = (args.format or out_path.suffix.lstrip(".").lower()).strip()
        if fmt == "parquet":
            df.to_parquet(out_path, index=False)
        elif fmt == "csv":
            df.to_csv(out_path, index=False)
        else:
            raise SystemExit("Unsupported output format. Use --format parquet|csv or a .parquet/.csv extension.")
    else:
        print(df.head(10).to_string(index=False))


def _print_dimension_names(dims: dict) -> None:
    for name, info in sorted(dims.items(), key=lambda kv: int(kv[1]["position"])):
        print(f"{info['position']:>2}. {name}")


def _resolve_dim_name_from_pos(dims: dict, pos: int) -> str:
    matches = [name for name, info in dims.items() if int(info["position"]) == pos]
    if not matches:
        available = sorted({int(info["position"]) for info in dims.values()})
        raise SystemExit(f"No dimension at position {pos}. Available positions: {available}")
    return matches[0]


def _print_dimension_values(
    dims: dict, 
    dim_name: Optional[str], 
    dim_pos: Optional[str], 
    limit: Optional[int]
) -> None:
    if (dim_name is None) and (dim_pos is None):
        raise SystemExit("Provide exactly one of --dim or --pos/-p.")
    
    if dim_pos is not None:
        dim_name = _resolve_dim_name_from_pos(dims, dim_pos)
    
    if dim_name not in dims:
        available = ", ".join(sorted(dims.keys()))
        raise SystemExit(f"Unknown dimension '{dim_name}'. Available: {available}")
    
    values = dims[dim_name]["values"]
    items = list(values.items())

    if limit is not None:
        items = items[:limit]

    print(f"# Dimension: {dim_name} (pos: {dims[dim_name]['position']})")
    
    for member_name, member_id in items:
        print(f"{member_id}\t{member_name}")


def _print_full_dimension_map(dims: dict, out: Optional[str]):
    payload = json.dumps(dims, ensure_ascii=False, indent=2)

    if out:
        with open(out, "w", encoding="utf-8") as f:
            f.write(payload)
    else:
        print(payload)


def main(argv=None):
    p = argparse.ArgumentParser(prog="statcan-wds")
    sub = p.add_subparsers(dest="cmd", required=True)

    # Preview dimensions
    p_dims = sub.add_parser("dims", help="List dimension names for a cube PID")
    p_dims.add_argument("pid", help="StatCan productId (PID)")

    # Preview values
    p_vals = sub.add_parser("values", help="List memeber values for a dimension")
    p_vals.add_argument("pid", help="StatCan productId (PID)")

    group = p_vals.add_mutually_exclusive_group(required=True)
    group.add_argument("--dim", help="English dimension name (dimensionNameEn)")
    group.add_argument("-p", "--pos", type=int, help="Dimension position (1-based)")
    
    p_vals.add_argument("--limit", type=int, default=50, help="Max members to show (defualt 50)")

    # Dump json (handy for scripting)
    p_json = sub.add_parser("dims-json", help="Dump full dimension map as JSON")
    p_json.add_argument("pid", help="StatCan productId (PID)")
    p_json.add_argument("--out", help="Write JSON to file instead of stdout")

    # Download data
    p_fetch = sub.add_parser("fetch", help="Fetch table data from YAML/JSON config")
    p_fetch.add_argument("config", help="Path to config (.yaml/.yml/.json)")
    p_fetch.add_argument("--start", help="Override start_ref_period (YYYY-MM-DD)")
    p_fetch.add_argument("--end", help="Override end_ref_period (YYYY-MM-DD)")
    p_fetch.add_argument("--out", help="Write results to file (csv/parquet). If omitted, prints a preview.")
    p_fetch.add_argument("--format", choices=["csv", "parquet"], help="Output format (defaults from --out extension)")

    args = p.parse_args(argv)

    if args.cmd == "fetch":
        cfg = _load_config(args.config)
        _fetch_data(args, cfg)
    else:
        metadata = get_cube_metatdata(args.pid)
        dims = inspect_dimensions(args.pid, metadata)

        if args.cmd == "dims":
            _print_dimension_names(dims)
        elif args.cmd == "values":
            _print_dimension_values(dims, args.dim, args.pos, args.limit)
        elif args.cmd == "dims-json":
            _print_full_dimension_map(dims, args.out)
