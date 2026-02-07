# statcan-wds
A lightweight, metadata-driven Python client and CLI for querying Statistics Canada's Web Data Service (WDS) using human-readable dimension names, instead of opaque coordinate vectors.

Designed for data engineers and analysts who want reproducible, config-driven StatCan data ingestion without manual filtering or CSV downloads.

## Why this exists
StatCan's WDS API exposes data via high-dimensional cubes that must be queried using numeric *coordinates* and *vector IDs*. While powerful, this makes programmatic access error-prone and difficult to explore.

This project provides a thin abstraction layer that:
- inspects cube metadata
- resolves human-readable dimension selections into valid WDS coordinates
- maps coordinates to vector IDs
- fetches time-series data into tidy DataFrames

- No UI scraping. No hard-coded IDs.

## Project structure
```graphql
statcan-wds/
├── statcan_wds/
│   ├── cli.py        # Command-line interface
│   ├── client.py    # Raw WDS HTTP calls
│   ├── metadata.py  # Cube metadata inspection
│   ├── resolver.py  # Label → coordinate → vector resolution
│   ├── fetch.py     # Data retrieval into DataFrames
│   └── errors.py
├── examples/
│   ├── cmhc.yaml
│   └── cpi_adjusted.yaml
├── examples.ipynb   # Notebook usage examples
├── pyproject.toml
└── README.md
```

## Installation (development)
Install the project in editable mode:
```bash
pip install -e .
```

This registers the `statcan-wds` CLI while keeping the code linked to your working directory, which is useful for development, experimentation, and review.

The command assumes you are already working inside a Python environment (e.g. `venv`, `conda`, Docker, etc.).

## Command-line usage
List available dimensions (with positions):
### Preview cube dimensions
```bash
statcan-wds dims 18100006
```

Example output:
```bash
 1. Geography
 2. Statistics
 3. North American Product Classification System (NAPCS)
```

### Inspect dimension members
By dimension name:
```bash
statcan-wds values 18100006 --dim "Geography" --limit 20
```

Or by dimension position:
```bash
statcan-wds values 18100006 --pos 1
```

### Dump full dimension metadata (JSON)
To stdout:
```bash
statcan-wds dims-json 18100006
```

Or write to disk:
```bash
statcan-wds dims-json 18100006 --out dims_18100006.json
```

### Fetching data with a config file
Data extraction is driven by a YAML or JSON config, defining:
- the StatCan table (`pid`)
- the dimension selections (`query`)
- optional reference-period bounds

Example YAML
```yaml
pid: "18100006"
query:
  - Geography: ["Canada", "Quebec"]
  - "North American Product Classification System (NAPCS)": "All merchandise"
start_ref_period: "2022-01-01"
end_ref_period: "2024-12-31"
```

### Fetch from the CLI
```bash
statcan-wds fetch examples/cpi_adjusted.yaml --out data/cpi.parquet
```

Supported outputs:
- `parquet`
- `csv`

If no output path is provided, a preview is printed to stdout.

## Python API usage
The library can also be used directly from Python or notebooks:
```python
from statcan_wds.fetch import get_table_data

df = get_table_data(
    pid="18100006",
    query_spec=[
        {"Geography": ["Canada", "Quebec"]},
        {"Statistics": "Consumer Price Index"}
    ],
    start_ref_period="2023-01-01",
    end_ref_period="2024-12-31",
)
```

The function returns a tidy pandas.DataFrame with:
- one column per selected dimension
- `REF_DATE`
- `VALUE`

## Reference period handling
At least one of `start_ref_period` or `end_ref_period` must be specified.

Dates must be in `YYYY-MM-DD` format.

## Non-goals
This project intentionally does not:
- manage credentials or secrets
- perform transformations or analytics
- cache data or metadata
- orchestrate workflows (Airflow, Dagster, etc.)

It focuses strictly on correct, reproducible data ingestion.
