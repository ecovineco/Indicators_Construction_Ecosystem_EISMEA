"""
Apply weights from the WeightMatrix to a raw extracted dataset.

Input:  a DataFrame with columns including Country, Year, NACE Code, and one or more value columns.
Output: same structure, but every value column is multiplied by the corresponding weight.

Core codes (C31, F, M71, N81) → weight = 1.0 (values unchanged)
Horizontal codes → weight looked up from the matrix by (Country, Year, data_type, NACE Code)
"""

import pandas as pd
from weights.matrix import WeightMatrix
from config.nace_codes import CORE_NACE_CODES


def apply_weights(
    df: pd.DataFrame,
    matrix: WeightMatrix,
    data_type: str,
    value_columns: list[str],
) -> pd.DataFrame:
    """
    Return a copy of df where each value column is multiplied by the
    appropriate weight from the matrix.

    Parameters
    ----------
    df : DataFrame
        Must contain columns: Country, Year, NACE Code, plus the value_columns.
    matrix : WeightMatrix
        Loaded weight matrix.
    data_type : str
        Key into the matrix (e.g. "employment", "turnover").
    value_columns : list[str]
        Column names that hold numeric values to be weighted.

    Returns
    -------
    DataFrame with same structure; value columns adjusted by weight.
    """
    out = df.copy()

    def _lookup_weight(row):
        nace = row["NACE Code"]
        if nace in CORE_NACE_CODES:
            return 1.0
        try:
            return matrix.get_weight(row["Country"], int(row["Year"]), data_type, nace)
        except KeyError:
            return None  # missing weight → will become NaN

    out["_weight"] = out.apply(_lookup_weight, axis=1)

    for col in value_columns:
        out[col] = out[col] * out["_weight"]

    out.drop(columns="_weight", inplace=True)
    return out
