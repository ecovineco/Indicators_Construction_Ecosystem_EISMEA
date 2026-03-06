"""
Data processing module.

Creates indicators from the extracted (and already weighted) datasets.
This module is **fully decoupled** from the extraction and weights
modules — it reads the extraction output Excel and produces its own
indicator output.

Processing will be implemented later.  For now this module provides
a stub :func:`run_all_processing` entry point that the main pipeline
can call safely.
"""

import os
import pandas as pd


# ---------------------------------------------------------------------------
# Registry: indicator name → processor function
#
# Processor functions receive extracted datasets (dict of DataFrames)
# and return indicator DataFrames.
# ---------------------------------------------------------------------------

PROCESSORS: dict = {
    # "employment_indicator": process_employment_indicator,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_all_processing(
    extraction_output_path: str,
    processing_output_path: str,
) -> dict[str, pd.DataFrame]:
    """Run all registered processors and save results.

    Reads the extracted datasets from *extraction_output_path*,
    applies each registered processor, and saves the resulting
    indicators to *processing_output_path*.

    Args:
        extraction_output_path: Path to the Excel file produced by
            the extraction module (one sheet per dataset).
        processing_output_path: Path where the indicator output
            Excel file will be saved.

    Returns:
        Dictionary mapping indicator names to their DataFrames.
        Currently empty (processing not yet implemented).
    """
    if not PROCESSORS:
        print("\nPROCESSING: no processors registered yet — skipping.")
        return {}

    # Load extracted datasets
    extracted = pd.read_excel(
        extraction_output_path, sheet_name=None, engine="openpyxl"
    )
    print(f"\nLoaded {len(extracted)} extracted dataset(s) for processing.")

    results: dict[str, pd.DataFrame] = {}
    for name, processor_fn in PROCESSORS.items():
        print(f"\n[{name}]")
        results[name] = processor_fn(extracted)

    # Save results
    if results:
        os.makedirs(os.path.dirname(processing_output_path) or ".", exist_ok=True)
        with pd.ExcelWriter(processing_output_path, engine="openpyxl") as writer:
            for name, df in results.items():
                sheet = name.replace("_", " ").title()[:31]
                df.to_excel(writer, sheet_name=sheet, index=False)
        print(f"\nProcessed indicators saved to {processing_output_path}")

    return results