# Construction Ecosystem Data Pipeline

Extracts Eurostat data, applies construction-sector weights, and produces clean Excel outputs.

## Architecture

```
construction_ecosystem/
├── config/
│   └── nace_codes.py              # Central NACE code definitions & labels
├── extraction/
│   ├── clients/                   # Reusable API clients (one per data source)
│   │   ├── __init__.py
│   │   └── eurostat.py            # Eurostat Statistics API (JSON-stat 2.0)
│   │   # └── oecd.py              # (future) OECD.Stat client
│   │   # └── worldbank.py         # (future) World Bank client
│   ├── __init__.py                # Extractor registry
│   ├── base.py                    # Abstract base class for all extractors
│   └── employment_by_sex.py       # ← first extractor (lfsa_egan22d)
├── processing/
│   ├── __init__.py                # Processor registry
│   └── employment_by_sex.py       # ← first processor
├── weights/
│   ├── __init__.py
│   ├── matrix.py                  # WeightMatrix class (store, lookup, load/save)
│   └── apply.py                   # apply_weights() — multiplies values by weights
├── output/                        # Generated Excel files
│   ├── 01_raw_datasets.xlsx       # One sheet per raw dataset
│   ├── 02_processed_datasets.xlsx # One sheet per weighted dataset
│   └── weights_matrix.xlsx        # Weight matrix (one sheet per data type)
├── main.py                        # Entry point
└── README.md
```

## How It Works

### Weight Logic
- **Core codes** (C31, F, M71, N81) → weight is always **1.0** (full value counts toward construction)
- **Horizontal codes** (C25, C28, C33, E36–E39, M69, M70, M72, N77, N78) → weight is **0–1**, varying by **country**, **year**, and **data type** (employment, turnover, etc.)
- The `weights_matrix.xlsx` stores these variable weights
- The pipeline multiplies each data point by its weight to get the construction-sector contribution

### Pipeline Steps
1. **Extract** — each extractor fetches one Eurostat dataset via the Statistics API
2. **Weight** — loads `weights_matrix.xlsx` (or creates a blank template on first run)
3. **Process** — each processor applies weights to its corresponding raw dataset
4. **Export** — saves three Excel files

## Quick Start

```bash
pip install requests pandas openpyxl

# First run: extracts data + creates blank weight template
python main.py --init-weights

# Fill in weights_matrix.xlsx with your weights...

# Full run: extract + apply weights + export
python main.py

# Extract only (no weighting):
python main.py --extract-only
```

## Adding a New Dataset

### 1. Create an extractor in `extraction/`

Pick the right client for the data source, call it, and shape the output:

```python
# extraction/turnover.py
from extraction.base import BaseExtractor
from extraction.clients.eurostat import fetch_eurostat, find_time_col
from config.nace_codes import API_NACE_CODES, NACE_LABELS

class TurnoverExtractor(BaseExtractor):
    dataset_label = "Turnover"

    def extract(self):
        raw, geo_labels = fetch_eurostat(
            dataset_code="sbs_na_ind_r2",
            params=[("indic_sb", "V12110")],
            nace_codes=API_NACE_CODES,
        )
        time_col = find_time_col(raw)
        # ... shape into output DataFrame with Country, Year, NACE Code, etc ...
        return out
```

If the data comes from a different source, use a different client:

```python
# extraction/clients/oecd.py   — you'd create this
def fetch_oecd(dataset_code, params, ...) -> (pd.DataFrame, dict):
    ...

# extraction/some_oecd_dataset.py
from extraction.clients.oecd import fetch_oecd
```

### 2. Register it in `extraction/__init__.py`

```python
from .turnover import TurnoverExtractor

EXTRACTORS = {
    "employment_by_sex": EmploymentBySexExtractor,
    "turnover":          TurnoverExtractor,        # ← add here
}
```

### 3. Create a processor in `processing/`

```python
# processing/turnover.py
from weights import apply_weights, WeightMatrix

def process_turnover(raw_df, matrix):
    return apply_weights(raw_df, matrix, data_type="turnover", value_columns=["Turnover"])
```

### 4. Register it in `processing/__init__.py`

```python
PROCESSORS = {
    "employment_by_sex": process_employment_by_sex,
    "turnover":          process_turnover,          # ← add here
}
```

### 5. Add a weight sheet
Either add a "turnover" sheet manually to `weights_matrix.xlsx`, or add `"turnover"` to the `data_types` list in `main.py` so it gets auto-created on `--init-weights`.

That's it — `python main.py` will now extract, weight, and export the new dataset alongside existing ones.
