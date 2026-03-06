"""
Construction Ecosystem Data Pipeline
======================================
Entry point that runs the full pipeline:

1. EXTRACT  — fetch raw data, apply weights, reshape to wide format,
              save to ``output/extracted_datasets.xlsx``
2. PROCESS  — create indicators from extracted datasets (TODO),
              save to ``output/processed_indicators.xlsx``

Usage:
    python main.py                — run full pipeline
    python main.py --extract-only — only extract, skip processing
"""

import os
import sys

from extraction import run_all_extractions
from processing import run_all_processing

OUTPUT_DIR = "output"
EXTRACTION_OUTPUT = os.path.join(OUTPUT_DIR, "extracted_datasets.xlsx")
PROCESSING_OUTPUT = os.path.join(OUTPUT_DIR, "processed_indicators.xlsx")


def main():
    """Run the full pipeline: extraction then processing."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Step 1: Extract → weight → reshape → save
    run_all_extractions(EXTRACTION_OUTPUT)

    if "--extract-only" in sys.argv:
        print("\n--extract-only: skipping processing.")
        return

    # Step 2: Process indicators (stub for now)
    run_all_processing(EXTRACTION_OUTPUT, PROCESSING_OUTPUT)

    print("\n" + "=" * 60)
    print("PIPELINE COMPLETE")
    print("=" * 60)
    print(f"  {EXTRACTION_OUTPUT}")
    if os.path.exists(PROCESSING_OUTPUT):
        print(f"  {PROCESSING_OUTPUT}")


if __name__ == "__main__":
    main()