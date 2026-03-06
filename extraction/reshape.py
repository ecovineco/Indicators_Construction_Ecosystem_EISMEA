"""
Reshape extracted long-format data into the standard wide-format output.

Converts a long-format DataFrame (one row per country/year/NACE code)
into a wide-format DataFrame where each NACE code becomes its own
value column.

Wide-format output columns
--------------------------
::

    Country Code | Country Name | Year | [extra dims] |
    Value for NACE code C25 | ... | Value for NACE code F(F41,F42,F43) |
    ... | Total Value | Unit

Special handling for NACE code F:
    F41, F42, and F43 are consolidated into a single column named
    ``"Value for NACE code F(F41,F42,F43)"``.  The module handles
    three scenarios:

    - Only sub-codes (F41/F42/F43) are present → summed into F.
    - Only the aggregate F is present → used directly.
    - Both exist → sub-codes are summed; the existing F row is dropped
      to avoid double-counting.
"""

import pandas as pd

from config.nace_codes import ALL_NACE_CODES, F_SUB_CODES


# ---------------------------------------------------------------------------
# Column-name constants
# ---------------------------------------------------------------------------

NACE_COLUMN_PREFIX = "Value for NACE code"
"""Prefix used for every NACE value column in the wide format."""

F_AGGREGATED_COLUMN_NAME = f"{NACE_COLUMN_PREFIX} F(F41,F42,F43)"
"""Column name for the consolidated F code (sum of F41 + F42 + F43)."""

TOTAL_VALUE_COLUMN = "Total Value"
"""Column holding the row-wise sum of all non-NaN NACE value columns."""

UNIT_COLUMN = "Unit"
"""Column holding the measurement unit string."""

# Standard id columns that every dataset has
BASE_ID_COLUMNS = ["Country Code", "Country Name", "Year"]

# NACE codes that appear as columns in the wide output (sorted)
_WIDE_FORMAT_NACE_CODES = sorted(ALL_NACE_CODES)


def _nace_column_name(nace_code: str) -> str:
    """Return the wide-format column name for a NACE code.

    Args:
        nace_code: A NACE code (e.g. ``"C25"``, ``"F"``).

    Returns:
        Column name string, e.g. ``"Value for NACE code C25"``
        or ``"Value for NACE code F(F41,F42,F43)"``.
    """
    if nace_code == "F":
        return F_AGGREGATED_COLUMN_NAME
    return f"{NACE_COLUMN_PREFIX} {nace_code}"


# Pre-compute the ordered list of NACE value column names
ORDERED_NACE_VALUE_COLUMNS = [
    _nace_column_name(code) for code in _WIDE_FORMAT_NACE_CODES
]
"""NACE value columns in the canonical display order."""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def reshape_to_wide(
    df: pd.DataFrame,
    extra_id_columns: list[str] | None = None,
    unit: str = "",
) -> pd.DataFrame:
    """Reshape a long-format extraction DataFrame into wide format.

    Args:
        df: Long-format DataFrame with columns ``Country Code``,
            ``Country Name``, ``Year``, ``NACE Code``, ``Value``,
            and any extra dimension columns.
        extra_id_columns: Names of additional dimension columns
            beyond ``Country Code``, ``Country Name``, ``Year``
            (e.g. ``["Sex"]``).  These become row identifiers in
            the pivot.
        unit: Measurement unit string placed in the ``Unit`` column
            (e.g. ``"Persons"``).

    Returns:
        Wide-format DataFrame with columns:
        ``Country Code``, ``Country Name``, ``Year``,
        ``[extra dims]``, one ``Value for NACE code …`` column per
        NACE code, ``Total Value``, ``Unit``.
    """
    df = df.copy()
    id_cols = BASE_ID_COLUMNS + (extra_id_columns or [])

    # -- Step 1: Consolidate F41/F42/F43 into a single "F" row ------------
    df = _consolidate_f_codes(df, id_cols)

    # -- Step 2: Pivot NACE Code → columns ---------------------------------
    wide = df.pivot_table(
        index=id_cols,
        columns="NACE Code",
        values="Value",
        aggfunc="sum",
    )

    # Flatten the column index and reset
    wide.columns = [_nace_column_name(c) for c in wide.columns]
    wide = wide.reset_index()

    # -- Step 3: Ensure all expected NACE columns exist --------------------
    for col_name in ORDERED_NACE_VALUE_COLUMNS:
        if col_name not in wide.columns:
            wide[col_name] = float("nan")

    # -- Step 4: Calculate Total Value (sum of non-NaN NACE columns) -------
    nace_cols_present = [c for c in ORDERED_NACE_VALUE_COLUMNS if c in wide.columns]
    wide[TOTAL_VALUE_COLUMN] = wide[nace_cols_present].sum(axis=1, min_count=1)

    # -- Step 5: Add Unit column -------------------------------------------
    wide[UNIT_COLUMN] = unit

    # -- Step 6: Order columns canonically ---------------------------------
    final_columns = (
        id_cols
        + ORDERED_NACE_VALUE_COLUMNS
        + [TOTAL_VALUE_COLUMN, UNIT_COLUMN]
    )
    # Only keep columns that actually exist
    final_columns = [c for c in final_columns if c in wide.columns]
    wide = wide[final_columns].copy()

    wide.sort_values(id_cols, inplace=True)
    wide.reset_index(drop=True, inplace=True)

    return wide


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _consolidate_f_codes(
    df: pd.DataFrame, id_cols: list[str]
) -> pd.DataFrame:
    """Merge F41, F42, F43 rows into a single F row per group.

    Handles three scenarios:
        - Only sub-codes present → sum into F.
        - Only F present → keep as-is.
        - Both present → sum sub-codes into F, drop the original F
          rows to avoid double-counting.

    Args:
        df: Long-format DataFrame with a ``NACE Code`` column.
        id_cols: Columns that identify a unique group (e.g.
            ``["Country Code", "Country Name", "Year", "Sex"]``).

    Returns:
        DataFrame with F41/F42/F43 replaced by a single F row per group.
    """
    has_subcodes = df["NACE Code"].isin(F_SUB_CODES).any()
    has_f_direct = (df["NACE Code"] == "F").any()

    # Rows that are neither F nor its sub-codes
    other_rows = df[~df["NACE Code"].isin(["F"] + F_SUB_CODES)]

    if has_subcodes:
        # Sum F41 + F42 + F43 into F
        f_sub = df[df["NACE Code"].isin(F_SUB_CODES)]
        f_agg = f_sub.groupby(id_cols, as_index=False)["Value"].sum()
        f_agg["NACE Code"] = "F"
        return pd.concat([other_rows, f_agg], ignore_index=True)

    if has_f_direct:
        f_rows = df[df["NACE Code"] == "F"]
        return pd.concat([other_rows, f_rows], ignore_index=True)

    # Neither F nor sub-codes → return everything except F-related
    return other_rows.reset_index(drop=True)