"""
Weight matrix for construction ecosystem shares.

Structure:
    Each weight is identified by (country, year, data_type, nace_code) → float [0–1]
    - Core codes (C31, F, M71, N81) always return 1.0
    - Horizontal codes look up from the stored matrix

Storage format (Excel):
    One sheet per data_type (e.g. "employment", "turnover", "enterprises").
    Rows = countries, Columns = NACE codes, with a Year column to allow
    different weights per year.

    Country | Year | C25  | C28  | C33  | E36  | ...
    Austria | 2020 | 0.12 | 0.08 | 0.15 | 0.05 | ...
    Belgium | 2020 | 0.10 | 0.07 | 0.18 | 0.04 | ...
"""

import pandas as pd
from config.nace_codes import CORE_NACE_CODES, HORIZONTAL_NACE_CODES, ALL_NACE_CODES

WEIGHTS_FILE = "output/weights_matrix.xlsx"


class WeightMatrix:
    """
    In-memory weight store.

    Internal storage: dict keyed by data_type → DataFrame with columns:
        Country, Year, <one col per horizontal NACE code>
    """

    def __init__(self):
        # data_type -> DataFrame
        self._tables: dict[str, pd.DataFrame] = {}

    # ── Lookup ──────────────────────────────────────────────────

    def get_weight(self, country: str, year: int, data_type: str, nace_code: str) -> float:
        """Return the weight for a single data point. Core codes always return 1.0."""
        if nace_code in CORE_NACE_CODES:
            return 1.0

        if nace_code not in HORIZONTAL_NACE_CODES:
            raise ValueError(f"Unknown NACE code: {nace_code}")

        table = self._tables.get(data_type)
        if table is None:
            raise KeyError(f"No weights loaded for data_type='{data_type}'")

        match = table[(table["Country"] == country) & (table["Year"] == year)]
        if match.empty:
            raise KeyError(f"No weight for {country}/{year}/{data_type}/{nace_code}")

        return float(match.iloc[0][nace_code])

    def get_weights_for_dataset(self, data_type: str) -> pd.DataFrame | None:
        """Return the full weight table for a data_type, or None."""
        return self._tables.get(data_type)

    # ── Mutate ──────────────────────────────────────────────────

    def set_table(self, data_type: str, df: pd.DataFrame):
        """
        Store a weight table for a data_type.
        df must have columns: Country, Year, and one column per horizontal NACE code.
        """
        required = {"Country", "Year"} | set(HORIZONTAL_NACE_CODES)
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Weight table missing columns: {missing}")
        self._tables[data_type] = df.copy()

    def set_weight(self, country: str, year: int, data_type: str, nace_code: str, weight: float):
        """Set a single weight value. Creates the table row if needed."""
        if nace_code in CORE_NACE_CODES:
            return  # core codes are always 1, nothing to store

        if data_type not in self._tables:
            cols = ["Country", "Year"] + HORIZONTAL_NACE_CODES
            self._tables[data_type] = pd.DataFrame(columns=cols)

        table = self._tables[data_type]
        mask = (table["Country"] == country) & (table["Year"] == year)

        if mask.any():
            self._tables[data_type].loc[mask, nace_code] = weight
        else:
            new_row = {c: 0.0 for c in HORIZONTAL_NACE_CODES}
            new_row["Country"] = country
            new_row["Year"] = year
            new_row[nace_code] = weight
            self._tables[data_type] = pd.concat(
                [table, pd.DataFrame([new_row])], ignore_index=True
            )

    # ── Persist ─────────────────────────────────────────────────

    def save(self, path: str = WEIGHTS_FILE):
        """Save all weight tables to Excel (one sheet per data_type)."""
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            for data_type, table in sorted(self._tables.items()):
                sheet = data_type[:31]  # Excel sheet name max 31 chars
                table.to_excel(writer, sheet_name=sheet, index=False)
        print(f"  Weights saved to {path} ({len(self._tables)} sheets)")

    def load(self, path: str = WEIGHTS_FILE):
        """Load weight tables from Excel."""
        sheets = pd.read_excel(path, sheet_name=None, engine="openpyxl")
        self._tables = {}
        for sheet_name, df in sheets.items():
            df["Year"] = df["Year"].astype(int)
            self._tables[sheet_name] = df
        print(f"  Weights loaded from {path} ({len(self._tables)} sheets)")

    # ── Utility ─────────────────────────────────────────────────

    def list_data_types(self) -> list[str]:
        return list(self._tables.keys())

    def create_template(self, data_type: str, countries: list[str], years: list[int]):
        """Create an empty weight table pre-filled with 0.0 for a data_type."""
        rows = []
        for country in countries:
            for year in years:
                row = {"Country": country, "Year": year}
                row.update({code: 0.0 for code in HORIZONTAL_NACE_CODES})
                rows.append(row)
        self._tables[data_type] = pd.DataFrame(rows)

    def __repr__(self):
        types = ", ".join(self._tables.keys()) or "(empty)"
        return f"WeightMatrix(data_types=[{types}])"
