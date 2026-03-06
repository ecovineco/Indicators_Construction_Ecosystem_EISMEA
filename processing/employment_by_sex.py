"""
Process: Employment by sex
Applies construction ecosystem weights to raw employment data.
"""

import pandas as pd
from weights import apply_weights, WeightMatrix


def process_employment_by_sex(raw_df: pd.DataFrame, matrix: WeightMatrix) -> pd.DataFrame:
    """
    Takes the raw employment-by-sex DataFrame and returns a weighted version.
    The 'Amount of People Employed' column is multiplied by each NACE code's
    weight for the corresponding country/year.

    Core codes (C31, F, M71, N81) → unchanged (weight=1)
    Horizontal codes → multiplied by their share attributable to construction
    """
    weighted = apply_weights(
        df=raw_df,
        matrix=matrix,
        data_type="employment",
        value_columns=["Amount of People Employed"],
    )

    # Round to whole persons after weighting
    weighted["Amount of People Employed"] = weighted["Amount of People Employed"].round(0)

    return weighted
