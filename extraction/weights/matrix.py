"""
Weight matrix for construction ecosystem shares.

Stores and retrieves the share of each horizontal NACE code's economic
activity that is attributable to the construction ecosystem, organised
by country, year, and data type.

Weight logic:
    - **Core NACE codes** (C31, F, M71, N81) always return weight = 1.0.
      These sectors are fully within the construction ecosystem.
    - **Horizontal NACE codes** (C25, C28, C33, E36--E39, M69, M70, M72,
      N77, N78) have variable weights between 0.0 and 1.0, looked up from
      stored tables that vary by country, year, and data type.

Storage format (Excel file -- one sheet per data type):
    Each sheet contains a long-format table with columns::

        Country Code | Country Name | Year | Horizontal Code | Weight
        | Associated Primary Construction NACE Codes

    Example row::

        AT | Austria | 2020 | C25 | 0.154 | C31, F41, F42, F43, M71, N81

Data-type keys and their corresponding Excel sheet names:

    ==================  ====================
    Internal key        Sheet name
    ==================  ====================
    turnover            Turnover
    persons_employed    Persons Employed
    employees_fte       Employees in FTEs
    enterprises         Enterprises
    other               Other
    ==================  ====================

Backward-compatible aliases:
    ``"employment"`` resolves to ``"persons_employed"`` so that existing
    processor code continues to work without changes.
"""

import pandas as pd

from config.nace_codes import CORE_NACE_CODES, HORIZONTAL_NACE_CODES
from .loader import (
    COL_COUNTRY_CODE,
    COL_COUNTRY_NAME,
    COL_YEAR,
    COL_HORIZONTAL_CODE,
    COL_WEIGHT,
    COL_ASSOCIATED_PRIMARY_CODES,
    WEIGHT_TABLE_COLUMNS,
    COUNTRY_CODE_TO_NAME,
    COUNTRY_NAME_TO_CODE,
)


# ---------------------------------------------------------------------------
# Data-type  <->  Excel sheet name mappings
# ---------------------------------------------------------------------------

DATA_TYPE_TO_SHEET_NAME: dict[str, str] = {
    "turnover": "Turnover",
    "persons_employed": "Persons Employed",
    "employees_fte": "Employees in FTEs",
    "enterprises": "Enterprises",
    "other": "Other",
}
"""Maps each internal data-type key to a human-readable Excel sheet name."""

SHEET_NAME_TO_DATA_TYPE: dict[str, str] = {
    v: k for k, v in DATA_TYPE_TO_SHEET_NAME.items()
}
"""Reverse mapping: Excel sheet name -> internal data-type key."""

_DATA_TYPE_ALIASES: dict[str, str] = {
    "employment": "persons_employed",
    "value_added": "turnover",
}
"""Aliases for backward compatibility with existing processor code.

When ``get_weight`` or other methods receive a data-type string listed
here, it is silently resolved to the canonical key on the right.
"""

DEFAULT_WEIGHTS_PATH = "output/weights_matrix.xlsx"
"""Default file path used by :meth:`WeightMatrix.save` and
:meth:`WeightMatrix.load` when no explicit path is provided."""


class WeightMatrix:
    """In-memory weight store for construction ecosystem shares.

    Manages a collection of weight tables, one per data type.  Each table
    is a :class:`~pandas.DataFrame` with columns matching
    :data:`weights.loader.WEIGHT_TABLE_COLUMNS`.

    Attributes:
        _tables: Internal dictionary mapping canonical data-type keys
            (e.g. ``"turnover"``) to their weight DataFrames.
    """

    def __init__(self):
        """Initialise an empty WeightMatrix with no loaded tables."""
        self._tables: dict[str, pd.DataFrame] = {}

    # ------------------------------------------------------------------
    # Data-type resolution
    # ------------------------------------------------------------------

    def _resolve_data_type(self, data_type: str) -> str:
        """Resolve a data-type string to its canonical internal key.

        Handles backward-compatible aliases so that, for example,
        ``"employment"`` is transparently mapped to ``"persons_employed"``.

        Args:
            data_type: Data-type key, possibly an alias.

        Returns:
            The canonical data-type key.
        """
        return _DATA_TYPE_ALIASES.get(data_type, data_type)

    # ------------------------------------------------------------------
    # Lookup
    # ------------------------------------------------------------------

    def get_weight(
        self, country: str, year: int, data_type: str, nace_code: str
    ) -> float:
        """Return the construction ecosystem weight for a single data point.

        Core NACE codes (C31, F, M71, N81) always return ``1.0`` without
        consulting stored tables.  Horizontal codes are looked up by
        matching on *country name* first (since the extraction pipeline
        produces full names like ``"Austria"``), falling back to
        *country code* (e.g. ``"AT"``).

        Args:
            country: Country identifier -- either the full name
                (e.g. ``"Austria"``) or the ISO 2-letter code
                (e.g. ``"AT"``).  Full names are tried first.
            year: Calendar year (e.g. ``2020``).
            data_type: Data-type key (e.g. ``"employment"``,
                ``"turnover"``).  Aliases are resolved automatically.
            nace_code: NACE code to look up (e.g. ``"C25"``).

        Returns:
            Weight as a float, typically in the range [0.0, 1.0].
            Core codes always return ``1.0``.

        Raises:
            ValueError: If *nace_code* is not a recognised core or
                horizontal code.
            KeyError: If no weight table is loaded for the resolved
                *data_type*, or if no matching row exists for the
                given (country, year, nace_code) combination.
        """
        if nace_code in CORE_NACE_CODES:
            return 1.0

        if nace_code not in HORIZONTAL_NACE_CODES:
            raise ValueError(f"Unknown NACE code: {nace_code}")

        resolved_type = self._resolve_data_type(data_type)

        table = self._tables.get(resolved_type)
        if table is None:
            raise KeyError(f"No weights loaded for data_type='{data_type}'")

        # Try lookup by country name first (matches extraction output)
        match = table[
            (table[COL_COUNTRY_NAME] == country)
            & (table[COL_YEAR] == year)
            & (table[COL_HORIZONTAL_CODE] == nace_code)
        ]

        # Fallback: try by country code
        if match.empty:
            match = table[
                (table[COL_COUNTRY_CODE] == country)
                & (table[COL_YEAR] == year)
                & (table[COL_HORIZONTAL_CODE] == nace_code)
            ]

        if match.empty:
            raise KeyError(
                f"No weight for {country}/{year}/{data_type}/{nace_code}"
            )

        return float(match.iloc[0][COL_WEIGHT])

    def get_weights_for_dataset(self, data_type: str) -> pd.DataFrame | None:
        """Return the full weight table for a data type, or ``None``.

        Args:
            data_type: Data-type key (e.g. ``"turnover"``).
                Aliases are resolved automatically.

        Returns:
            DataFrame with weight data for the requested data type,
            or ``None`` if no table is loaded for that type.
        """
        resolved = self._resolve_data_type(data_type)
        return self._tables.get(resolved)

    # ------------------------------------------------------------------
    # Mutate
    # ------------------------------------------------------------------

    def set_table(self, data_type: str, df: pd.DataFrame):
        """Store a complete weight table for a data type.

        Validates that the DataFrame contains the required columns
        before storing.  The DataFrame is copied to prevent external
        mutation of internal state.

        Args:
            data_type: Internal data-type key (e.g. ``"turnover"``).
                Aliases are resolved automatically.
            df: DataFrame whose columns must be a superset of
                ``{Country Code, Country Name, Year,
                Horizontal Code, Weight}``.

        Raises:
            ValueError: If any of the required columns are missing.
        """
        resolved = self._resolve_data_type(data_type)

        required_columns = {
            COL_COUNTRY_CODE,
            COL_COUNTRY_NAME,
            COL_YEAR,
            COL_HORIZONTAL_CODE,
            COL_WEIGHT,
        }
        missing = required_columns - set(df.columns)
        if missing:
            raise ValueError(f"Weight table missing columns: {missing}")

        self._tables[resolved] = df.copy()

    def set_weight(
        self,
        country: str,
        year: int,
        data_type: str,
        nace_code: str,
        weight: float,
    ):
        """Set a single weight value, creating the table or row if needed.

        Core NACE codes are silently ignored (they are always 1.0 and
        are never stored).  If a row for the given (country, year,
        nace_code) already exists, the weight is updated in place;
        otherwise a new row is appended.

        Args:
            country: Country name (e.g. ``"Austria"``) or ISO 2-letter
                code (e.g. ``"AT"``).
            year: Calendar year.
            data_type: Data-type key (e.g. ``"turnover"``).
            nace_code: Horizontal NACE code (e.g. ``"C25"``).
            weight: Weight value to store (typically 0.0--1.0).
        """
        if nace_code in CORE_NACE_CODES:
            return  # core codes are always 1.0; nothing to store

        resolved = self._resolve_data_type(data_type)

        if resolved not in self._tables:
            self._tables[resolved] = pd.DataFrame(columns=WEIGHT_TABLE_COLUMNS)

        # Resolve country to both code and name
        country_code = COUNTRY_NAME_TO_CODE.get(country, country)
        country_name = COUNTRY_CODE_TO_NAME.get(country_code, country)

        table = self._tables[resolved]
        mask = (
            (table[COL_COUNTRY_CODE] == country_code)
            & (table[COL_YEAR] == year)
            & (table[COL_HORIZONTAL_CODE] == nace_code)
        )

        if mask.any():
            self._tables[resolved].loc[mask, COL_WEIGHT] = weight
        else:
            new_row = {
                COL_COUNTRY_CODE: country_code,
                COL_COUNTRY_NAME: country_name,
                COL_YEAR: year,
                COL_HORIZONTAL_CODE: nace_code,
                COL_WEIGHT: weight,
                COL_ASSOCIATED_PRIMARY_CODES: "",
            }
            self._tables[resolved] = pd.concat(
                [table, pd.DataFrame([new_row])], ignore_index=True
            )

    # ------------------------------------------------------------------
    # Persist
    # ------------------------------------------------------------------

    def save(self, path: str = DEFAULT_WEIGHTS_PATH):
        """Save all weight tables to an Excel file (one sheet per data type).

        Sheet names are determined by :data:`DATA_TYPE_TO_SHEET_NAME`.
        If a data-type key has no mapping, the key itself is used as the
        sheet name.

        Args:
            path: Output file path.  Defaults to
                ``"output/weights_matrix.xlsx"``.
        """
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            for data_type in sorted(self._tables.keys()):
                table = self._tables[data_type]
                sheet_name = DATA_TYPE_TO_SHEET_NAME.get(
                    data_type, data_type
                )[:31]  # Excel limits sheet names to 31 characters
                table.to_excel(writer, sheet_name=sheet_name, index=False)
        print(f"  Weights saved to {path} ({len(self._tables)} sheets)")

    def load(self, path: str = DEFAULT_WEIGHTS_PATH):
        """Load weight tables from an Excel file.

        Reads every sheet from the file and maps sheet names back to
        internal data-type keys using :data:`SHEET_NAME_TO_DATA_TYPE`.
        Sheets whose names are not in the mapping are stored under their
        original sheet name as-is.

        Args:
            path: Path to the Excel file to load.  Defaults to
                ``"output/weights_matrix.xlsx"``.
        """
        sheets = pd.read_excel(path, sheet_name=None, engine="openpyxl")
        self._tables = {}
        for sheet_name, df in sheets.items():
            data_type = SHEET_NAME_TO_DATA_TYPE.get(sheet_name, sheet_name)
            df[COL_YEAR] = df[COL_YEAR].astype(int)
            self._tables[data_type] = df
        print(f"  Weights loaded from {path} ({len(self._tables)} sheets)")

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    def list_data_types(self) -> list[str]:
        """Return a list of all loaded data-type keys.

        Returns:
            List of canonical data-type keys, e.g.
            ``["enterprises", "persons_employed", "turnover"]``.
        """
        return list(self._tables.keys())

    def create_template(
        self, data_type: str, countries: list[str], years: list[int]
    ):
        """Create an empty weight table pre-filled with 0.0.

        Generates a row for every combination of (country, year,
        horizontal NACE code), all initialised with weight = 0.0 and
        an empty string for associated primary codes.

        Args:
            data_type: Data-type key (e.g. ``"turnover"``).
                Aliases are resolved automatically.
            countries: List of country names (e.g.
                ``["Austria", "Belgium"]``).
            years: List of calendar years (e.g. ``[2020, 2021]``).
        """
        resolved = self._resolve_data_type(data_type)
        rows = []
        for country_name in countries:
            country_code = COUNTRY_NAME_TO_CODE.get(country_name, "")
            for year in years:
                for horizontal_code in HORIZONTAL_NACE_CODES:
                    rows.append({
                        COL_COUNTRY_CODE: country_code,
                        COL_COUNTRY_NAME: country_name,
                        COL_YEAR: year,
                        COL_HORIZONTAL_CODE: horizontal_code,
                        COL_WEIGHT: 0.0,
                        COL_ASSOCIATED_PRIMARY_CODES: "",
                    })
        self._tables[resolved] = pd.DataFrame(rows)

    def __repr__(self) -> str:
        """Return a string representation showing loaded data types.

        Returns:
            String like
            ``"WeightMatrix(data_types=[turnover, persons_employed])"``.
        """
        types = ", ".join(sorted(self._tables.keys())) or "(empty)"
        return f"WeightMatrix(data_types=[{types}])"