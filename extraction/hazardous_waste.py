"""
Extract: Generation of hazardous waste by the construction industry (F).

Dataset: ``env_wasgen``
Source:   Eurostat — Generation of waste by waste category, hazardousness
          and NACE Rev. 2 activity
Unit:    Tonnes (no conversion needed)
Weight data type: ``other``

Filters applied:
  - ``hazard`` = ``HAZ`` (Hazardous waste only)
  - ``waste`` = ``TOTAL`` (All waste categories combined)
  - ``unit`` = ``T`` (Tonnes)

Note: This dataset provides NACE codes at the section level only.
Only "F" (Construction) is extracted. Data is biennial (every 2 years).
"""

import pandas as pd

from extraction.base import BaseExtractor
from extraction.clients.eurostat import fetch_eurostat, find_time_col
from config.nace_codes import API_NACE_CODES_1LETTER


class HazardousWasteExtractor(BaseExtractor):
    """Extractor for hazardous waste generation from Eurostat (env_wasgen).

    Produces a long-format DataFrame with one row per
    (country, year, NACE code) combination.

    Class attributes:
        dataset_label: ``"Hazardous Waste"``
        weight_data_type: ``"other"``
        unit: ``"Tonnes"``
        extra_id_columns: ``[]``
    """

    dataset_label = "Hazardous Waste"
    weight_data_type = "other"
    unit = "Tonnes"
    extra_id_columns: list[str] = []

    def extract(self) -> pd.DataFrame:
        """Fetch hazardous waste generation data from Eurostat.

        Returns:
            DataFrame with columns: ``Country Code``, ``Country Name``,
            ``Year``, ``NACE Code``, ``Value``.

            Values are in tonnes. Data is biennial (every 2 years).
        """
        raw, geo_labels = fetch_eurostat(
            dataset_code="env_wasgen",
            params=[
                ("hazard", "HAZ"),
                ("waste", "TOTAL"),
                ("unit", "T"),
            ],
            nace_codes=API_NACE_CODES_1LETTER,
            aggregate_f=False,
        )

        time_col = find_time_col(raw)

        out = pd.DataFrame({
            "Country Code": raw["geo"],
            "Country Name": raw["geo"].map(lambda g: geo_labels.get(g, g)),
            "Year": raw[time_col].astype(int),
            "NACE Code": raw["nace_r2"],
            "Value": raw["value"],
        })

        out.sort_values(
            ["Country Code", "Year", "NACE Code"], inplace=True
        )
        out.reset_index(drop=True, inplace=True)
        print(f"  Hazardous Waste: {len(out)} rows extracted")
        return out
