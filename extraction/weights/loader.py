"""
Loader for raw construction ecosystem weight shares from the source CSV.

This module reads the source CSV file containing weight shares broken down
by primary construction NACE code, aggregates them into summed weights per
horizontal NACE code, and returns structured DataFrames ready for storage
in the WeightMatrix.

The source CSV contains one row per (primary code, country, year, horizontal
code) combination, with separate weight columns for each data type (turnover,
persons employed, etc.). This loader:

    1. Groups rows by (country, year, horizontal code) within each data type.
    2. Sums the weight values across all primary construction NACE codes.
    3. Records which primary codes contributed to each summed weight.
    4. Fills in any missing combinations with weight = 0.0.

Primary Construction NACE Codes (the core sector codes whose individual
contributions are summed to produce horizontal code weights):
    C31  -- Manufacture of furniture
    F41  -- Construction of buildings
    F42  -- Civil engineering
    F43  -- Specialised construction activities
    M71  -- Architectural and engineering activities
    N81  -- Services to buildings and landscape activities

Horizontal NACE Codes (the codes that receive variable weights):
    C25, C28, C33, E36, E37, E38, E39, M69, M70, M72, N77, N78

Typical usage::

    from weights.loader import load_weights_from_csv

    tables = load_weights_from_csv("weights/raw_weight_shares.csv")
    # tables["turnover"]         -> DataFrame with summed turnover weights
    # tables["persons_employed"] -> DataFrame with summed employment weights
"""

import pandas as pd

from config.nace_codes import HORIZONTAL_NACE_CODES


# ---------------------------------------------------------------------------
# Source CSV column names
# ---------------------------------------------------------------------------

SOURCE_COL_PRIMARY_NACE = "NACE"
"""Column in the source CSV holding the primary construction NACE code."""

SOURCE_COL_COUNTRY_CODE = "geo"
"""Column in the source CSV holding the ISO 2-letter country code."""

SOURCE_COL_YEAR = "TIME_PERIOD"
"""Column in the source CSV holding the calendar year."""

SOURCE_COL_HORIZONTAL_NACE = "Horizontal NACE code"
"""Column in the source CSV holding the horizontal NACE code being weighted."""


# ---------------------------------------------------------------------------
# Mapping: CSV weight column name  ->  internal data-type key
# ---------------------------------------------------------------------------

WEIGHT_COLUMN_TO_DATA_TYPE = {
    "Turnover weight": "turnover",
    "Persons employed weight": "persons_employed",
    "Employees in FTEs weight": "employees_fte",
    "Amount of Enterprises weight": "enterprises",
}
"""Maps each weight column in the source CSV to a short internal key.

These internal keys are used as dictionary keys in the WeightMatrix and
as the basis for Excel sheet names in the storage file.
"""


# ---------------------------------------------------------------------------
# Output column names  (used in every structured weight table)
# ---------------------------------------------------------------------------

COL_COUNTRY_CODE = "Country Code"
"""ISO 2-letter country code (e.g. 'AT')."""

COL_COUNTRY_NAME = "Country Name"
"""Full country name (e.g. 'Austria')."""

COL_YEAR = "Year"
"""Calendar year as integer."""

COL_HORIZONTAL_CODE = "Horizontal Code"
"""Horizontal NACE code whose weight is stored (e.g. 'C25')."""

COL_WEIGHT = "Weight"
"""Summed weight across all primary construction NACE codes."""

COL_ASSOCIATED_PRIMARY_CODES = "Associated Primary Construction NACE Codes"
"""Comma-separated sorted list of primary codes that contributed."""

WEIGHT_TABLE_COLUMNS = [
    COL_COUNTRY_CODE,
    COL_COUNTRY_NAME,
    COL_YEAR,
    COL_HORIZONTAL_CODE,
    COL_WEIGHT,
    COL_ASSOCIATED_PRIMARY_CODES,
]
"""Ordered list of columns in every structured weight table."""


# ---------------------------------------------------------------------------
# Country code  <->  country name mapping  (EU member states)
# ---------------------------------------------------------------------------

COUNTRY_CODE_TO_NAME = {
    "AT": "Austria",
    "BE": "Belgium",
    "BG": "Bulgaria",
    "CY": "Cyprus",
    "CZ": "Czech Republic",
    "DE": "Germany",
    "DK": "Denmark",
    "EE": "Estonia",
    "EL": "Greece",
    "ES": "Spain",
    "FI": "Finland",
    "FR": "France",
    "HR": "Croatia",
    "HU": "Hungary",
    "IE": "Ireland",
    "IT": "Italy",
    "LT": "Lithuania",
    "LU": "Luxembourg",
    "LV": "Latvia",
    "MT": "Malta",
    "NL": "Netherlands",
    "PL": "Poland",
    "PT": "Portugal",
    "RO": "Romania",
    "SE": "Sweden",
    "SI": "Slovenia",
    "SK": "Slovakia",
}
"""Maps ISO 2-letter country codes to full country names."""

COUNTRY_NAME_TO_CODE = {name: code for code, name in COUNTRY_CODE_TO_NAME.items()}
"""Reverse mapping: full country name -> ISO 2-letter code."""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_weights_from_csv(csv_path: str) -> dict[str, pd.DataFrame]:
    """Parse the raw weight shares CSV and produce aggregated weight tables.

    For each data type (turnover, persons_employed, etc.) this function:

        1. Groups rows by (country code, year, horizontal NACE code).
        2. Sums the weight values across all primary construction NACE codes
           (C31, F41, F42, F43, M71, N81) within each group.
        3. Records which primary codes contributed to each summed weight.
        4. Creates rows with weight = 0.0 for any (country, year, horizontal
           code) combinations that are absent from the source data.

    Args:
        csv_path: Absolute or relative path to the source CSV file
            containing raw weight shares broken down by primary NACE code.

    Returns:
        Dictionary mapping internal data-type keys to DataFrames.

        Keys: ``"turnover"``, ``"persons_employed"``,
        ``"employees_fte"``, ``"enterprises"``.

        Each DataFrame has the columns defined in
        :data:`WEIGHT_TABLE_COLUMNS`:

        - **Country Code** *(str)*: ISO 2-letter code (e.g. ``"AT"``).
        - **Country Name** *(str)*: Full name (e.g. ``"Austria"``).
        - **Year** *(int)*: Calendar year.
        - **Horizontal Code** *(str)*: Horizontal NACE code (e.g. ``"C25"``).
        - **Weight** *(float)*: Summed weight across all primary codes.
        - **Associated Primary Construction NACE Codes** *(str)*:
          Comma-separated sorted list of contributing primary codes, or
          an empty string when no source rows exist.

    Raises:
        FileNotFoundError: If *csv_path* does not exist.
        KeyError: If expected columns are missing from the CSV.

    Example::

        >>> tables = load_weights_from_csv("weights/raw_weight_shares.csv")
        >>> tables["turnover"].head(1)
           Country Code Country Name  Year Horizontal Code   Weight  ...
        0           AT      Austria  2008             C25  0.15450  ...
    """
    raw_csv = pd.read_csv(csv_path)

    # Determine the full set of years present in the source data
    all_years_in_data = sorted(raw_csv[SOURCE_COL_YEAR].unique())
    all_country_codes = sorted(COUNTRY_CODE_TO_NAME.keys())
    all_horizontal_codes = HORIZONTAL_NACE_CODES

    tables: dict[str, pd.DataFrame] = {}

    for csv_weight_col, data_type_key in WEIGHT_COLUMN_TO_DATA_TYPE.items():
        tables[data_type_key] = _aggregate_single_data_type(
            raw_csv=raw_csv,
            weight_column=csv_weight_col,
            all_country_codes=all_country_codes,
            all_years=all_years_in_data,
            all_horizontal_codes=all_horizontal_codes,
        )

    return tables


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _aggregate_single_data_type(
    raw_csv: pd.DataFrame,
    weight_column: str,
    all_country_codes: list[str],
    all_years: list[int],
    all_horizontal_codes: list[str],
) -> pd.DataFrame:
    """Aggregate weights for one data type and fill missing combinations.

    Groups the raw CSV by (country, year, horizontal code), sums the
    specified weight column across primary codes, and ensures every
    possible (country, year, horizontal code) combination has a row.

    Args:
        raw_csv: Full raw CSV DataFrame (all rows, all columns).
        weight_column: Name of the CSV weight column to sum
            (e.g. ``"Turnover weight"``).
        all_country_codes: Complete list of country codes to include.
        all_years: Complete list of years to include.
        all_horizontal_codes: Complete list of horizontal NACE codes.

    Returns:
        DataFrame with columns matching :data:`WEIGHT_TABLE_COLUMNS`,
        sorted by (Country Code, Year, Horizontal Code).
    """
    group_keys = [SOURCE_COL_COUNTRY_CODE, SOURCE_COL_YEAR, SOURCE_COL_HORIZONTAL_NACE]

    # -- Step 1: Sum weights across primary codes per group ----------------
    summed_weights = (
        raw_csv
        .groupby(group_keys, as_index=False)[weight_column]
        .sum()
        .rename(columns={
            SOURCE_COL_COUNTRY_CODE: COL_COUNTRY_CODE,
            SOURCE_COL_YEAR: COL_YEAR,
            SOURCE_COL_HORIZONTAL_NACE: COL_HORIZONTAL_CODE,
            weight_column: COL_WEIGHT,
        })
    )

    # -- Step 2: Collect associated primary codes per group ----------------
    associated_codes = (
        raw_csv
        .groupby(group_keys, as_index=False)[SOURCE_COL_PRIMARY_NACE]
        .agg(lambda codes: ", ".join(sorted(codes.unique())))
        .rename(columns={
            SOURCE_COL_COUNTRY_CODE: COL_COUNTRY_CODE,
            SOURCE_COL_YEAR: COL_YEAR,
            SOURCE_COL_HORIZONTAL_NACE: COL_HORIZONTAL_CODE,
            SOURCE_COL_PRIMARY_NACE: COL_ASSOCIATED_PRIMARY_CODES,
        })
    )

    # -- Step 3: Merge sums with associated codes --------------------------
    aggregated = summed_weights.merge(
        associated_codes,
        on=[COL_COUNTRY_CODE, COL_YEAR, COL_HORIZONTAL_CODE],
        how="left",
    )

    # -- Step 4: Build full cartesian product to fill gaps -----------------
    full_combos = pd.MultiIndex.from_product(
        [all_country_codes, all_years, all_horizontal_codes],
        names=[COL_COUNTRY_CODE, COL_YEAR, COL_HORIZONTAL_CODE],
    )
    full_df = pd.DataFrame(index=full_combos).reset_index()

    # -- Step 5: Left-join aggregated data onto the full grid --------------
    result = full_df.merge(
        aggregated,
        on=[COL_COUNTRY_CODE, COL_YEAR, COL_HORIZONTAL_CODE],
        how="left",
    )
    result[COL_WEIGHT] = result[COL_WEIGHT].fillna(0.0)
    result[COL_ASSOCIATED_PRIMARY_CODES] = (
        result[COL_ASSOCIATED_PRIMARY_CODES].fillna("")
    )

    # -- Step 6: Add country names and finalise column order ---------------
    result[COL_COUNTRY_NAME] = result[COL_COUNTRY_CODE].map(COUNTRY_CODE_TO_NAME)

    result = result[WEIGHT_TABLE_COLUMNS].copy()
    result.sort_values(
        [COL_COUNTRY_CODE, COL_YEAR, COL_HORIZONTAL_CODE],
        inplace=True,
    )
    result.reset_index(drop=True, inplace=True)

    return result