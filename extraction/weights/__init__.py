"""
Weights sub-module for the extraction pipeline.

Manages construction ecosystem weight shares internally.  The
WeightMatrix is loaded **once** (lazily, on first use) and reused
for all subsequent calls.

Public API
----------
``apply_weights(df, data_type)``
    Apply weights to a long-format DataFrame.  The matrix is loaded
    automatically — callers only supply the data and the data-type key.

``get_weight_matrix()``
    Return the loaded :class:`WeightMatrix` singleton (mainly for
    inspection / debugging).

Weight source priority (checked on first call):
    1. Structured Excel at ``output/weights_matrix.xlsx``.
    2. Raw source CSV at ``extraction/weights/raw_weight_shares.csv``
       — parsed, saved to Excel, then loaded.
    3. If neither exists a warning is printed and all horizontal
       weights default to ``NaN``.

Typical usage::

    from extraction.weights import apply_weights

    weighted_df = apply_weights(raw_df, data_type="persons_employed")
"""

import os
import pandas as pd

from .matrix import WeightMatrix
from .loader import (
    load_weights_from_csv,
    COL_COUNTRY_CODE,
    COL_COUNTRY_NAME,
    COL_YEAR,
    COL_HORIZONTAL_CODE,
    COL_WEIGHT,
    COL_ASSOCIATED_PRIMARY_CODES,
    WEIGHT_TABLE_COLUMNS,
)
from .apply import apply_weight_column


# ---------------------------------------------------------------------------
# Paths (resolved relative to *this* file so they work from any cwd)
# ---------------------------------------------------------------------------

_WEIGHTS_DIR = os.path.dirname(os.path.abspath(__file__))
"""Absolute path to the extraction/weights/ directory."""

_CSV_SOURCE_PATH = os.path.join(_WEIGHTS_DIR, "raw_weight_shares.csv")
"""Path to the raw source CSV with weight shares by primary NACE code."""

_EXCEL_STORAGE_PATH = os.path.join(
    _WEIGHTS_DIR, "..", "..", "output", "weights_matrix.xlsx"
)
"""Path to the structured Excel file (one sheet per data type)."""


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------

_matrix: WeightMatrix | None = None


def _extrapolate_2024(matrix: WeightMatrix) -> None:
    """Extrapolate 2024 weights from 2021-2023 using a weighted average.

    For each (country, horizontal code) combination, the 2024 weight is
    computed as a normalized weighted average:
        0.70 * value_2023 + 0.20 * value_2022 + 0.10 * value_2021

    If any of the three source years is missing (no row or weight == 0.0),
    its contribution is dropped and the remaining weights are re-normalized
    so they still sum to 1.0. If none of the three years exist, 2024 is
    set to 0.0.

    Existing 2024 rows are overwritten.

    Args:
        matrix: The WeightMatrix instance to modify in place.
    """
    source_years = {2023: 0.70, 2022: 0.20, 2021: 0.10}

    for data_type in matrix.list_data_types():
        table = matrix.get_weights_for_dataset(data_type)
        if table is None or table.empty:
            continue

        # Remove any existing 2024 rows
        table = table[table[COL_YEAR] != 2024].copy()

        # Build lookup: (country_code, horizontal_code, year) -> row
        indexed = table.set_index(
            [COL_COUNTRY_CODE, COL_HORIZONTAL_CODE, COL_YEAR]
        )

        new_rows = []
        groups = table.groupby([COL_COUNTRY_CODE, COL_COUNTRY_NAME, COL_HORIZONTAL_CODE])
        for (country_code, country_name, h_code), _group in groups:
            weighted_sum = 0.0
            total_weight = 0.0
            associated_codes_parts = []

            for year, w in source_years.items():
                key = (country_code, h_code, year)
                if key in indexed.index:
                    val = indexed.loc[key, COL_WEIGHT]
                    # Handle potential duplicate index by taking first
                    if hasattr(val, '__iter__'):
                        val = val.iloc[0]
                    val = float(val)
                    if val > 0.0:
                        weighted_sum += w * val
                        total_weight += w
                        # Collect associated codes from this year
                        assoc = indexed.loc[key, COL_ASSOCIATED_PRIMARY_CODES]
                        if hasattr(assoc, '__iter__') and not isinstance(assoc, str):
                            assoc = assoc.iloc[0]
                        if assoc:
                            associated_codes_parts.append(str(assoc))

            if total_weight > 0.0:
                extrapolated_weight = weighted_sum / total_weight
            else:
                extrapolated_weight = 0.0

            # Merge and deduplicate associated primary codes
            all_codes = set()
            for part in associated_codes_parts:
                for code in part.split(","):
                    code = code.strip()
                    if code:
                        all_codes.add(code)
            associated_str = ", ".join(sorted(all_codes))

            new_rows.append({
                COL_COUNTRY_CODE: country_code,
                COL_COUNTRY_NAME: country_name,
                COL_YEAR: 2024,
                COL_HORIZONTAL_CODE: h_code,
                COL_WEIGHT: extrapolated_weight,
                COL_ASSOCIATED_PRIMARY_CODES: associated_str,
            })

        if new_rows:
            new_df = pd.concat(
                [table, pd.DataFrame(new_rows, columns=WEIGHT_TABLE_COLUMNS)],
                ignore_index=True,
            )
            new_df.sort_values(
                [COL_COUNTRY_CODE, COL_YEAR, COL_HORIZONTAL_CODE],
                inplace=True,
            )
            new_df.reset_index(drop=True, inplace=True)
            matrix.set_table(data_type, new_df)

    print("  Extrapolated 2024 weights from 2021-2023 (70/20/10 blend).")


def _generate_other_sheet(matrix: WeightMatrix) -> None:
    """Generate the 'other' weight sheet with all weights set to 0.0.

    Collects all unique (Country Code, Country Name, Year, Horizontal Code)
    combinations from every other loaded sheet, then creates a single table
    with weight = 0.0 for each combination.

    Args:
        matrix: The WeightMatrix instance to modify in place.
    """
    all_combos = set()
    for data_type in matrix.list_data_types():
        if data_type == "other":
            continue
        table = matrix.get_weights_for_dataset(data_type)
        if table is None or table.empty:
            continue
        for _, row in table.iterrows():
            all_combos.add((
                row[COL_COUNTRY_CODE],
                row[COL_COUNTRY_NAME],
                int(row[COL_YEAR]),
                row[COL_HORIZONTAL_CODE],
            ))

    if not all_combos:
        return

    rows = [
        {
            COL_COUNTRY_CODE: cc,
            COL_COUNTRY_NAME: cn,
            COL_YEAR: yr,
            COL_HORIZONTAL_CODE: hc,
            COL_WEIGHT: 0.0,
            COL_ASSOCIATED_PRIMARY_CODES: "",
        }
        for cc, cn, yr, hc in sorted(all_combos)
    ]
    other_df = pd.DataFrame(rows, columns=WEIGHT_TABLE_COLUMNS)
    matrix.set_table("other", other_df)
    print(f"    other: {len(other_df)} rows (all weights = 0.0)")


def _ensure_matrix_loaded() -> None:
    """Lazily load the WeightMatrix on first access.

    Priority:
        1. Structured Excel already exists → load it.
        2. Source CSV exists → parse, save Excel, load.
        3. Neither → print warning; matrix stays empty.
    """
    global _matrix
    if _matrix is not None:
        return

    excel_path = os.path.normpath(_EXCEL_STORAGE_PATH)
    csv_path = os.path.normpath(_CSV_SOURCE_PATH)

    _matrix = WeightMatrix()

    loaded = False

    # Try loading from structured Excel first
    if os.path.exists(excel_path):
        try:
            _matrix.load(excel_path)
            # Remove deprecated data types that may exist in old Excel files
            for deprecated_key in ("value_added", "Value Added", "other", "Other"):
                if deprecated_key in _matrix._tables:
                    del _matrix._tables[deprecated_key]
            loaded = True
        except (PermissionError, OSError) as exc:
            print(f"  WARNING: Cannot read {excel_path} ({exc}), trying CSV...")

    # Fall back to building from source CSV
    if not loaded and os.path.exists(csv_path):
        print(f"\n  Building weight matrix from {csv_path} ...")
        tables = load_weights_from_csv(csv_path)
        for data_type_key, weight_table in tables.items():
            _matrix.set_table(data_type_key, weight_table)
            print(f"    {data_type_key}: {len(weight_table)} rows")
        loaded = True

    if not loaded:
        print("  WARNING: No weight data found — horizontal codes will be NaN.")
        return

    # Generate the "Other" sheet: all combos from existing sheets, weight = 0.0
    _generate_other_sheet(_matrix)

    # Extrapolate 2024 weights from recent years
    _extrapolate_2024(_matrix)

    # Save the matrix (with 2024) to the output Excel
    try:
        os.makedirs(os.path.dirname(excel_path), exist_ok=True)
        _matrix.save(excel_path)
    except (PermissionError, OSError) as exc:
        print(f"  WARNING: Could not save Excel ({exc}), using in-memory only.")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def apply_weights(df: pd.DataFrame, data_type: str) -> pd.DataFrame:
    """Apply construction ecosystem weights to a long-format DataFrame.

    The WeightMatrix is loaded automatically on first call.  Callers
    only need to provide the extracted data and the data-type key.

    Args:
        df: Long-format DataFrame that **must** contain the columns
            ``Country Code``, ``Year``, ``NACE Code``, and ``Value``.
        data_type: Key identifying which weight sheet to use
            (e.g. ``"persons_employed"``, ``"turnover"``).  The alias
            ``"employment"`` is also accepted and resolves to
            ``"persons_employed"``.

    Returns:
        A copy of *df* where the ``Value`` column of horizontal NACE
        codes has been multiplied by the corresponding weight.  Core
        codes are unchanged (weight 1.0).
    """
    _ensure_matrix_loaded()
    return apply_weight_column(df, _matrix, data_type)


def get_weight_matrix() -> WeightMatrix:
    """Return the loaded WeightMatrix singleton.

    Useful for inspection, debugging, or direct lookups.  The matrix
    is loaded lazily if it hasn't been accessed yet.

    Returns:
        The module-level :class:`WeightMatrix` instance.
    """
    _ensure_matrix_loaded()
    return _matrix