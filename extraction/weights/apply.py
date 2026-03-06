"""
Apply construction ecosystem weights to an extracted dataset.

This module provides the internal weight-application logic used by
the weights package.  External code should call
:func:`extraction.weights.apply_weights` (the public wrapper in
``extraction/weights/__init__.py``) which manages the WeightMatrix
singleton automatically.

Weight rules:
    - **Core NACE codes** (C31, F, M71, N81) and their sub-codes
      (F41, F42, F43) always receive weight **1.0**.
    - **Horizontal NACE codes** (C25, C28, C33, E36--E39, M69, M70,
      M72, N77, N78) are multiplied by their looked-up weight from
      the WeightMatrix.
    - Any other NACE code receives ``None`` (becomes ``NaN``).

The function expects a standardised long-format DataFrame with at
least the columns ``"Country Code"``, ``"Year"``, ``"NACE Code"``,
and ``"Value"``.
"""

import pandas as pd

from .matrix import WeightMatrix
from config.nace_codes import CORE_NACE_CODES, HORIZONTAL_NACE_CODES, F_SUB_CODES


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FULL_WEIGHT_NACE_CODES = set(CORE_NACE_CODES) | set(F_SUB_CODES)
"""NACE codes that always receive weight 1.0 (core construction sector
plus the F sub-codes F41, F42, F43 which roll up into the core code F).
"""

STANDARD_COUNTRY_CODE_COL = "Country Code"
"""Expected column name holding the ISO 2-letter country code."""

STANDARD_YEAR_COL = "Year"
"""Expected column name holding the calendar year."""

STANDARD_NACE_CODE_COL = "NACE Code"
"""Expected column name holding the NACE classification code."""

STANDARD_VALUE_COL = "Value"
"""Expected column name holding the numeric value to be weighted."""


# ---------------------------------------------------------------------------
# Internal apply function
# ---------------------------------------------------------------------------

def apply_weight_column(
    df: pd.DataFrame,
    matrix: WeightMatrix,
    data_type: str,
) -> pd.DataFrame:
    """Multiply the *Value* column by the appropriate weight for each row.

    This is an **internal** helper.  External callers should use
    :func:`extraction.weights.apply_weights` instead, which manages
    the WeightMatrix automatically.

    Args:
        df: Long-format DataFrame with columns ``Country Code``,
            ``Year``, ``NACE Code``, and ``Value``.
        matrix: A loaded :class:`WeightMatrix` instance.
        data_type: Data-type key for the weight lookup
            (e.g. ``"persons_employed"``).

    Returns:
        A copy of *df* where the ``Value`` column has been multiplied
        by each row's weight.  Rows whose weight cannot be determined
        will have ``NaN`` in the ``Value`` column.
    """
    out = df.copy()

    def _lookup_weight(row: pd.Series) -> float | None:
        """Determine the weight for a single row."""
        nace = row[STANDARD_NACE_CODE_COL]

        # Core construction codes + F sub-codes → always 1.0
        if nace in FULL_WEIGHT_NACE_CODES:
            return 1.0

        # Horizontal codes → look up from the matrix
        if nace in HORIZONTAL_NACE_CODES:
            try:
                return matrix.get_weight(
                    row[STANDARD_COUNTRY_CODE_COL],
                    int(row[STANDARD_YEAR_COL]),
                    data_type,
                    nace,
                )
            except KeyError:
                return None  # missing weight → NaN

        # Unknown NACE code
        return None

    out["_weight"] = out.apply(_lookup_weight, axis=1)
    out[STANDARD_VALUE_COL] = out[STANDARD_VALUE_COL] * out["_weight"]
    out.drop(columns="_weight", inplace=True)
    return out