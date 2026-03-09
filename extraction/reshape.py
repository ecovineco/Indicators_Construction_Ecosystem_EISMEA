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

NACE_COLUMN_PREFIX = "Weighted value for NACE code"
"""Prefix used for every NACE value column in the wide format."""

UNWEIGHTED_COLUMN_PREFIX = "Unweighted value for NACE code"
"""Prefix used for every unweighted NACE value column in the wide format."""

F_AGGREGATED_COLUMN_NAME = f"{NACE_COLUMN_PREFIX} F(F41,F42,F43)"
"""Column name for the consolidated F code (sum of F41 + F42 + F43)."""

F_UNWEIGHTED_COLUMN_NAME = f"{UNWEIGHTED_COLUMN_PREFIX} F(F41,F42,F43)"
"""Column name for the unweighted consolidated F code."""

TOTAL_VALUE_COLUMN = "Total Weighted Value"
"""Column holding the row-wise sum of all non-NaN weighted NACE value columns."""

TOTAL_UNWEIGHTED_COLUMN = "Total Unweighted Value"
"""Column holding the row-wise sum of all non-NaN unweighted NACE value columns."""

UNIT_COLUMN = "Unit"
"""Column holding the measurement unit string."""

# Standard id columns that every dataset has
BASE_ID_COLUMNS = ["Country Code", "Country Name", "Year"]

# NACE codes that appear as columns in the wide output (sorted)
_WIDE_FORMAT_NACE_CODES = sorted(ALL_NACE_CODES)


def _nace_column_name(nace_code: str) -> str:
    """Return the wide-format weighted column name for a NACE code.

    Args:
        nace_code: A NACE code (e.g. ``"C25"``, ``"F"``).

    Returns:
        Column name string, e.g. ``"Value for NACE code C25"``
        or ``"Value for NACE code F(F41,F42,F43)"``.
    """
    if nace_code == "F":
        return F_AGGREGATED_COLUMN_NAME
    return f"{NACE_COLUMN_PREFIX} {nace_code}"


def _unweighted_column_name(nace_code: str) -> str:
    """Return the wide-format unweighted column name for a NACE code.

    Args:
        nace_code: A NACE code (e.g. ``"C25"``, ``"F"``).

    Returns:
        Column name string, e.g. ``"Unweighted value for NACE code C25"``
        or ``"Unweighted value for NACE code F(F41,F42,F43)"``.
    """
    if nace_code == "F":
        return F_UNWEIGHTED_COLUMN_NAME
    return f"{UNWEIGHTED_COLUMN_PREFIX} {nace_code}"


# Pre-compute the ordered list of NACE value column names
ORDERED_NACE_VALUE_COLUMNS = [
    _nace_column_name(code) for code in _WIDE_FORMAT_NACE_CODES
]
"""Weighted NACE value columns in the canonical display order."""

ORDERED_UNWEIGHTED_COLUMNS = [
    _unweighted_column_name(code) for code in _WIDE_FORMAT_NACE_CODES
]
"""Unweighted NACE value columns in the canonical display order."""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def reshape_to_wide(
    df: pd.DataFrame,
    extra_id_columns: list[str] | None = None,
    unit: str = "",
    unweighted_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Reshape a long-format extraction DataFrame into wide format.

    Args:
        df: Long-format weighted DataFrame with columns ``Country Code``,
            ``Country Name``, ``Year``, ``NACE Code``, ``Value``,
            and any extra dimension columns.
        extra_id_columns: Names of additional dimension columns
            beyond ``Country Code``, ``Country Name``, ``Year``
            (e.g. ``["Sex"]``).  These become row identifiers in
            the pivot.
        unit: Measurement unit string placed in the ``Unit`` column
            (e.g. ``"Persons"``).
        unweighted_df: Optional long-format DataFrame with the original
            unweighted values.  When provided, each weighted NACE column
            is followed by its unweighted counterpart in the output.

    Returns:
        Wide-format DataFrame with columns:
        ``Country Code``, ``Country Name``, ``Year``,
        ``[extra dims]``, interleaved weighted / unweighted NACE
        columns, ``Total Value``, ``Total Unweighted Value``, ``Unit``.
    """
    df = df.copy()
    id_cols = BASE_ID_COLUMNS + (extra_id_columns or [])

    # -- Step 1: Consolidate F41/F42/F43 into a single "F" row ------------
    df = _consolidate_f_codes(df, id_cols)

    # -- Step 2: Pivot weighted NACE Code → columns ------------------------
    wide = df.pivot_table(
        index=id_cols,
        columns="NACE Code",
        values="Value",
        aggfunc="sum",
    )
    wide.columns = [_nace_column_name(c) for c in wide.columns]
    wide = wide.reset_index()

    # -- Step 2b: Pivot unweighted values if provided ----------------------
    if unweighted_df is not None:
        uw = unweighted_df.copy()
        uw = _consolidate_f_codes(uw, id_cols)
        uw_wide = uw.pivot_table(
            index=id_cols,
            columns="NACE Code",
            values="Value",
            aggfunc="sum",
        )
        uw_wide.columns = [_unweighted_column_name(c) for c in uw_wide.columns]
        uw_wide = uw_wide.reset_index()
        wide = wide.merge(uw_wide, on=id_cols, how="left")

    # -- Step 3: Build ordered NACE column lists ----------------------------
    # Detect which NACE codes are actually present in the pivoted data.
    # Some datasets use 1-letter codes (C, E, F, M, N) instead of 2-digit.
    actual_weighted_cols = [
        c for c in wide.columns if c.startswith(NACE_COLUMN_PREFIX)
    ]
    actual_nace_codes = sorted(set(
        c.replace(NACE_COLUMN_PREFIX + " ", "")
        .replace("F(F41,F42,F43)", "F")
        for c in actual_weighted_cols
    ))

    # Use canonical order for standard codes; append any extra codes at end
    canonical_codes = _WIDE_FORMAT_NACE_CODES
    extra_codes = [c for c in actual_nace_codes if c not in canonical_codes]
    ordered_codes = canonical_codes + sorted(extra_codes)

    ordered_weighted = [_nace_column_name(c) for c in ordered_codes]
    ordered_unweighted = [_unweighted_column_name(c) for c in ordered_codes]

    # Ensure all expected NACE columns exist (fill missing with NaN)
    for col_name in ordered_weighted:
        if col_name not in wide.columns:
            wide[col_name] = float("nan")
    if unweighted_df is not None:
        for col_name in ordered_unweighted:
            if col_name not in wide.columns:
                wide[col_name] = float("nan")

    # -- Step 4: Calculate Total Values ------------------------------------
    nace_cols_present = [c for c in ordered_weighted if c in wide.columns]
    wide[TOTAL_VALUE_COLUMN] = wide[nace_cols_present].sum(axis=1, min_count=1)

    if unweighted_df is not None:
        uw_cols_present = [c for c in ordered_unweighted if c in wide.columns]
        wide[TOTAL_UNWEIGHTED_COLUMN] = wide[uw_cols_present].sum(
            axis=1, min_count=1
        )

    # -- Step 5: Add Unit column -------------------------------------------
    wide[UNIT_COLUMN] = unit

    # -- Step 6: Order columns canonically ---------------------------------
    # Interleave weighted and unweighted columns per NACE code
    nace_columns_ordered: list[str] = []
    for weighted_col, unweighted_col in zip(
        ordered_weighted, ordered_unweighted
    ):
        nace_columns_ordered.append(weighted_col)
        if unweighted_df is not None:
            nace_columns_ordered.append(unweighted_col)

    total_columns = [TOTAL_VALUE_COLUMN]
    if unweighted_df is not None:
        total_columns.append(TOTAL_UNWEIGHTED_COLUMN)

    final_columns = (
        id_cols
        + nace_columns_ordered
        + total_columns
        + [UNIT_COLUMN]
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