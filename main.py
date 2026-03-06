"""
Construction Ecosystem Data Pipeline
======================================
Entry point that runs the full pipeline:

1. EXTRACT  — fetch raw data from Eurostat (one extractor per dataset)
2. WEIGHT   — load the weights matrix
3. PROCESS  — apply weights to produce construction-sector-adjusted data
4. EXPORT   — save three Excel files:
       output/01_raw_datasets.xlsx        — one sheet per raw dataset
       output/02_processed_datasets.xlsx  — one sheet per weighted dataset
       output/weights_matrix.xlsx         — the weights matrix

Usage:
    python main.py                  — run full pipeline
    python main.py --extract-only   — only extract, skip processing
    python main.py --init-weights   — create empty weight template and exit
"""

import os
import sys
import pandas as pd

from extraction import EXTRACTORS
from processing import PROCESSORS
from weights import WeightMatrix

OUTPUT_DIR = "output"
RAW_FILE = os.path.join(OUTPUT_DIR, "01_raw_datasets.xlsx")
PROCESSED_FILE = os.path.join(OUTPUT_DIR, "02_processed_datasets.xlsx")
WEIGHTS_FILE = os.path.join(OUTPUT_DIR, "weights_matrix.xlsx")


def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


# ── Step 1: Extract ────────────────────────────────────────────

def run_extraction() -> dict[str, pd.DataFrame]:
    """Run all registered extractors. Returns {name: DataFrame}."""
    print("=" * 60)
    print("STEP 1: EXTRACTION")
    print("=" * 60)
    raw_data = {}
    for name, extractor_cls in EXTRACTORS.items():
        print(f"\n[{name}]")
        extractor = extractor_cls()
        raw_data[name] = extractor.extract()
    return raw_data


def save_raw(raw_data: dict[str, pd.DataFrame]):
    """Save all raw datasets to a single Excel file, one sheet per dataset."""
    with pd.ExcelWriter(RAW_FILE, engine="openpyxl") as writer:
        for name, df in raw_data.items():
            sheet = name.replace("_", " ").title()[:31]
            df.to_excel(writer, sheet_name=sheet, index=False)
    print(f"\nRaw data saved to {RAW_FILE}")


# ── Step 2: Weights ────────────────────────────────────────────

def load_or_create_weights(raw_data: dict[str, pd.DataFrame]) -> WeightMatrix:
    """Load weights from file, or create a template if none exists."""
    matrix = WeightMatrix()

    if os.path.exists(WEIGHTS_FILE):
        print(f"\nLoading weights from {WEIGHTS_FILE}")
        matrix.load(WEIGHTS_FILE)
    else:
        print(f"\nNo weights file found — creating template at {WEIGHTS_FILE}")
        # Gather all countries and years from extracted data to build templates
        all_countries = set()
        all_years = set()
        for df in raw_data.values():
            if "Country" in df.columns:
                all_countries.update(df["Country"].unique())
            if "Year" in df.columns:
                all_years.update(df["Year"].unique())

        countries = sorted(all_countries)
        years = sorted(all_years)

        # Create one template sheet per known data type
        data_types = ["employment", "turnover", "enterprises", "value_added"]
        for dt in data_types:
            matrix.create_template(dt, countries, years)

        matrix.save(WEIGHTS_FILE)
        print(f"  Template created with {len(countries)} countries × {len(years)} years")
        print(f"  Fill in the weights in {WEIGHTS_FILE}, then re-run the pipeline.")

    return matrix


# ── Step 3: Process ─────────────────────────────────────────────

def run_processing(raw_data: dict[str, pd.DataFrame], matrix: WeightMatrix) -> dict[str, pd.DataFrame]:
    """Run all registered processors. Returns {name: weighted DataFrame}."""
    print("\n" + "=" * 60)
    print("STEP 3: PROCESSING (applying weights)")
    print("=" * 60)
    processed = {}
    for name, processor_fn in PROCESSORS.items():
        if name not in raw_data:
            print(f"\n[{name}] SKIPPED — no raw data extracted")
            continue
        print(f"\n[{name}]")
        processed[name] = processor_fn(raw_data[name], matrix)
        print(f"  {name}: {len(processed[name])} weighted rows")
    return processed


def save_processed(processed: dict[str, pd.DataFrame]):
    """Save all processed datasets to a single Excel file."""
    with pd.ExcelWriter(PROCESSED_FILE, engine="openpyxl") as writer:
        for name, df in processed.items():
            sheet = name.replace("_", " ").title()[:31]
            df.to_excel(writer, sheet_name=sheet, index=False)
    print(f"\nProcessed data saved to {PROCESSED_FILE}")


# ── Main ────────────────────────────────────────────────────────

def main():
    ensure_output_dir()

    # Handle --init-weights flag
    if "--init-weights" in sys.argv:
        print("Initializing empty weight templates...")
        raw_data = run_extraction()
        save_raw(raw_data)
        load_or_create_weights(raw_data)
        print("\nDone. Fill in the weights Excel and re-run without --init-weights.")
        return

    # Step 1: Extract
    raw_data = run_extraction()
    save_raw(raw_data)

    if "--extract-only" in sys.argv:
        print("\n--extract-only: skipping processing.")
        return

    # Step 2: Load weights
    matrix = load_or_create_weights(raw_data)

    # Step 3: Process
    processed = run_processing(raw_data, matrix)
    save_processed(processed)

    print("\n" + "=" * 60)
    print("PIPELINE COMPLETE")
    print("=" * 60)
    print(f"  {RAW_FILE}")
    print(f"  {PROCESSED_FILE}")
    print(f"  {WEIGHTS_FILE}")


if __name__ == "__main__":
    main()
