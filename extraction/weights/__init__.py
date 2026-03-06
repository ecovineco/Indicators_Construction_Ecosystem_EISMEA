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
from .loader import load_weights_from_csv
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

    # Try loading from structured Excel first
    if os.path.exists(excel_path):
        try:
            _matrix.load(excel_path)
            return
        except (PermissionError, OSError) as exc:
            print(f"  WARNING: Cannot read {excel_path} ({exc}), trying CSV...")

    # Fall back to building from source CSV
    if os.path.exists(csv_path):
        print(f"\n  Building weight matrix from {csv_path} ...")
        tables = load_weights_from_csv(csv_path)
        for data_type_key, weight_table in tables.items():
            _matrix.set_table(data_type_key, weight_table)
            print(f"    {data_type_key}: {len(weight_table)} rows")
        # Try to save the Excel (may fail if directory/file is locked)
        try:
            os.makedirs(os.path.dirname(excel_path), exist_ok=True)
            _matrix.save(excel_path)
            print(f"  You may now delete {csv_path} if no longer needed.")
        except (PermissionError, OSError) as exc:
            print(f"  WARNING: Could not save Excel ({exc}), using in-memory only.")
        return

    print("  WARNING: No weight data found — horizontal codes will be NaN.")


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