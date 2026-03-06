"""
Data extraction module.

Orchestrates the full extraction pipeline for every registered dataset:

    1. **Extract** — fetch raw data from an external source (e.g. Eurostat).
    2. **Weight** — apply construction ecosystem weights to horizontal
       NACE codes (weights are managed internally by the weights sub-module).
    3. **Reshape** — pivot from long format to the standard wide format
       (one column per NACE code, with F41/F42/F43 consolidated into F).
    4. **Save** — write all datasets to a single Excel file (one sheet
       per dataset).

Adding a new dataset
--------------------
1. Create an extractor class in a new ``.py`` file in this package.
2. Register it in :data:`EXTRACTORS` below.
3. Run the pipeline — the new dataset will be extracted, weighted,
   reshaped, and saved automatically.
"""

import os
import pandas as pd

from .employment_by_sex import EmploymentBySexExtractor
from .weights import apply_weights
from .reshape import reshape_to_wide


# ---------------------------------------------------------------------------
# Registry: dataset name → extractor class
#
# The dictionary key is the dataset's unique
# identifier throughout the pipeline and becomes the Excel sheet name.
# ---------------------------------------------------------------------------

EXTRACTORS = {
    "employment_by_sex": EmploymentBySexExtractor,
    # "turnover":        TurnoverExtractor,
    # "enterprises":     EnterprisesExtractor,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_all_extractions(output_path: str) -> dict[str, pd.DataFrame]:
    """Run every registered extractor: extract → weight → reshape → save.

    For each dataset in :data:`EXTRACTORS`:

    1. Instantiates the extractor and calls :meth:`extract`.
    2. Applies construction ecosystem weights via
       :func:`extraction.weights.apply_weights` using the extractor's
       :attr:`weight_data_type`.
    3. Reshapes the result to wide format via
       :func:`extraction.reshape.reshape_to_wide`.
    4. Collects all wide-format DataFrames and saves them to a single
       Excel file at *output_path* (one sheet per dataset).

    Args:
        output_path: File path for the output Excel file
            (e.g. ``"output/extracted_datasets.xlsx"``).

    Returns:
        Dictionary mapping dataset names to their wide-format DataFrames.
    """
    print("=" * 60)
    print("EXTRACTION  (extract -> weight -> reshape)")
    print("=" * 60)

    results: dict[str, pd.DataFrame] = {}

    for dataset_name, extractor_cls in EXTRACTORS.items():
        print(f"\n[{dataset_name}]")

        extractor = extractor_cls()

        # Step 1: Extract raw long-format data
        raw_long = extractor.extract()

        # Step 2: Apply weights (internal singleton — no matrix argument)
        weighted_long = apply_weights(raw_long, extractor.weight_data_type)
        print(f"  Weights applied (data_type={extractor.weight_data_type})")

        # Step 3: Reshape to wide format
        wide = reshape_to_wide(
            weighted_long,
            extra_id_columns=extractor.extra_id_columns,
            unit=extractor.unit,
        )
        print(f"  Reshaped: {len(wide)} rows x {len(wide.columns)} columns")

        results[dataset_name] = wide

    # Step 4: Save all datasets to Excel
    _save_results(results, output_path)

    return results


def _save_results(results: dict[str, pd.DataFrame], output_path: str) -> None:
    """Save all extraction results to a single Excel file.

    Each dataset gets its own sheet.  Sheet names are derived from the
    dataset name (underscores replaced with spaces, title-cased, max
    31 characters to satisfy Excel limits).

    Args:
        results: Mapping of dataset names to wide-format DataFrames.
        output_path: Target Excel file path.
    """
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for name, df in results.items():
            sheet = name.replace("_", " ").title()[:31]
            df.to_excel(writer, sheet_name=sheet, index=False)

    print(f"\nExtracted datasets saved to {output_path}")