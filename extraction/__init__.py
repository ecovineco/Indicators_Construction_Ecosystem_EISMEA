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
from .employment_by_age_sex import EmploymentByAgeSexExtractor
from .turnover import TurnoverExtractor
from .enterprises_by_size import EnterprisesBySizeExtractor
from .training_participation import TrainingParticipationExtractor
from .rd_expenditure import RdExpenditureExtractor
from .hours_worked import HoursWorkedExtractor
from .fdi_inward import FdiInwardExtractor
from .investment_per_person import InvestmentPerPersonExtractor
from .ip_investment import IpInvestmentExtractor
from .education_level import EducationLevelExtractor
from .temporary_employment import TemporaryEmploymentExtractor
from .hazardous_waste import HazardousWasteExtractor
from .ict_usage import IctUsageExtractor
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
    "employment_by_age_sex": EmploymentByAgeSexExtractor,
    "turnover": TurnoverExtractor,
    "enterprises_by_size": EnterprisesBySizeExtractor,
    "training_participation": TrainingParticipationExtractor,
    "rd_expenditure": RdExpenditureExtractor,
    "hours_worked": HoursWorkedExtractor,
    "fdi_inward": FdiInwardExtractor,
    "investment_per_person": InvestmentPerPersonExtractor,
    "ip_investment": IpInvestmentExtractor,
    "education_level": EducationLevelExtractor,
    "temporary_employment": TemporaryEmploymentExtractor,
    "hazardous_waste": HazardousWasteExtractor,
    "ict_usage": IctUsageExtractor,
}


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# When True, datasets whose sheet already exists in the output Excel file
# are loaded from file and not re-extracted.  Set to False to force a full
# re-extraction of every dataset.
SKIP_EXISTING = True


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
    4. Saves the dataset to the Excel file immediately (one sheet per
       dataset), so that successfully extracted datasets are preserved
       even if a later extractor fails.

    When :data:`SKIP_EXISTING` is ``True``, datasets whose sheet already
    exists in the output file are loaded from disk and not re-extracted.

    Args:
        output_path: File path for the output Excel file
            (e.g. ``"output/extracted_datasets.xlsx"``).

    Returns:
        Dictionary mapping dataset names to their wide-format DataFrames.
    """
    print("=" * 60)
    print("EXTRACTION  (extract -> weight -> reshape -> save)")
    print("=" * 60)

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    results: dict[str, pd.DataFrame] = {}

    # Determine which sheets already exist in the output file
    existing_sheets: set[str] = set()
    if SKIP_EXISTING and os.path.exists(output_path):
        existing_file = pd.ExcelFile(output_path, engine="openpyxl")
        existing_sheets = set(existing_file.sheet_names)
        existing_file.close()

    # Use append mode if the file exists and we want to keep existing sheets,
    # otherwise create a new file from scratch.
    file_exists = os.path.exists(output_path)
    writer_mode = "a" if file_exists and SKIP_EXISTING else "w"
    writer_kwargs = {"if_sheet_exists": "replace"} if writer_mode == "a" else {}

    with pd.ExcelWriter(
        output_path, engine="openpyxl", mode=writer_mode, **writer_kwargs
    ) as writer:
        for dataset_name, extractor_cls in EXTRACTORS.items():
            sheet = dataset_name.replace("_", " ").title()[:31]

            # Skip datasets that are already in the file
            if SKIP_EXISTING and sheet in existing_sheets:
                print(f"\n[{dataset_name}]  Already in file — skipped")
                results[dataset_name] = pd.read_excel(
                    output_path, sheet_name=sheet, engine="openpyxl"
                )
                continue

            print(f"\n[{dataset_name}]")

            extractor = extractor_cls()

            # Step 1: Extract raw long-format data
            raw_long = extractor.extract()

            # Step 2: Apply weights (internal singleton — no matrix argument)
            weighted_long = apply_weights(raw_long, extractor.weight_data_type)
            print(f"  Weights applied (data_type={extractor.weight_data_type})")

            # Step 3: Reshape to wide format (include unweighted values)
            wide = reshape_to_wide(
                weighted_long,
                extra_id_columns=extractor.extra_id_columns,
                unit=extractor.unit,
                unweighted_df=raw_long,
            )
            print(f"  Reshaped: {len(wide)} rows x {len(wide.columns)} columns")

            # Step 4: Save to Excel immediately
            wide.to_excel(writer, sheet_name=sheet, index=False)
            print(f"  Saved to sheet '{sheet}'")

            results[dataset_name] = wide

    print(f"\nExtracted datasets saved to {output_path}")

    return results
