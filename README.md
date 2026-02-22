# statcan-wds
A lightweight, metadata-driven Python client and CLI for querying Statistics Canada's Web Data Service (WDS) using human-readable dimension names, instead of opaque coordinate vectors.

Designed for data engineers and analysts who want reproducible, config-driven StatCan data ingestion without manual filtering or CSV downloads.

## Why this exists
StatCan's WDS API exposes data via high-dimensional cubes that must be queried using numeric *coordinates* and *vector IDs*. While powerful, this makes programmatic access error-prone and difficult to explore.

This project provides a thin abstraction layer that:
- Inspects cube metadata.
- Resolves human-readable dimension selections into valid WDS coordinates.
- Automatically determines if a table requires **Time-Series** (Vector) or **Snapshot** (Cube) retrieval logic.
- Fetches data into tidy DataFrames with automatic label mapping.

---
## Installation
### Standard Installation
The easiest way to use statcan-wds in your project is to install it directly from GitHub:

```bash
pip install git+https://github.com/cdngouma/statcan-wds.git
```

### Development Mode
If you plan on contributing to the code or experimenting with the source, clone the repository and install it in editable mode:

```bash
git clone https://github.com/cdngouma/statcan-wds.git
cd statcan-wds
pip install -e .
```
---
## Data Handling Logic
### Time-Series vs. Census Snapshots
The client automatically switches between two retrieval methods:
- Time-Series (Vector ID): Used for standard tables (CPI, GDP) to fetch historical ranges.
- Snapshots (Cube-Coordinate): Used for Census data (PIDs starting with 98) or any data slice lacking a Vector ID.

### Reference Period Logic
Format: All dates must follow the `ISO 8601` format: `YYYY-MM-DD`.
Missing Dates:
- `ref_start` only -> Fetches from start date to latest date in the table.
- `ref_end` only -> Fetches a single point for that date.
- None provided -> Fetches the entire range available in the table metadata.

---
## Python API usage
The library can be used directly in scripts or Jupyter notebooks:

```python
from statcan_wds.fetch import get_table_data

df = get_table_data(
    pid="18100006",
    query_spec=[
        {"Geography": ["Canada", "Quebec"]},
        {"Statistics": "Consumer Price Index"}
    ],
    ref_start="2023-01-01" # Automatically fetches up to latest date
)
```
The function returns a tidy pandas.DataFrame with:
- One column per selected dimension (using human-readable labels).
- `REF_DATE`
- `VALUE`

---
## Command-line usage
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
Create a `config.yaml`:
```yaml
pid: "18100006"
query:
  - Geography: ["Canada", "Quebec"]
  - "North American Product Classification System (NAPCS)": "All merchandise"
ref_start: "2022-01-01"
```

Run the fetch:
```bash
statcan-wds fetch config.yaml --out data/results.parquet
```
Supported outputs:
- `parquet`
- `csv`

---
## Project structure
```plaintext
statcan-wds/
├── statcan_wds/
│   ├── cli.py        # Command-line interface
│   ├── client.py    # Raw WDS HTTP calls
│   ├── metadata.py  # Cube metadata inspection
│   ├── resolver.py  # Label -> coordinate -> vector resolution
│   ├── fetch.py     # Data retrieval into DataFrames
│   └── errors.py
├── examples/
│   ├── cmhc.yaml
│   └── cpi_adjusted.yaml
├── examples.ipynb   # Notebook usage examples
├── pyproject.toml
└── README.md
```

---

## Official Web Data Service (WDS) User Guide
[Documentation](https://www.statcan.gc.ca/en/developers/wds/user-guide)