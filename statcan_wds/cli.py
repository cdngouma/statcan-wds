from typing import Optional
import argparse
import json
from .metadata import inspect_dimensions


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

    print(f"# Dimension: {dim_name} (position {dims[dim_name]['position']})")
    
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

    args = p.parse_args(argv)

    dims = inspect_dimensions(args.pid)

    if args.cmd == "dims":
        _print_dimension_names(dims)
    elif args.cmd == "values":
        _print_dimension_values(dims, args.dim, args.pos, args.limit)
    elif args.cmd == "dims-json":
        _print_full_dimension_map(dims, args.out)
