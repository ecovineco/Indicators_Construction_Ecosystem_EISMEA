"""
Extract: Data analytics by NACE Rev. 2 activity and NUTS 2 region.

Dataset: ``isoc_eb_dan2``
Source:   Eurostat — Data analytics by NACE Rev. 2 activity and NUTS 2 region
Indicator: ``E_DAOWN`` — Enterprises where data analytics for the enterprise
           is performed by own employees
Unit:    Percentage of enterprises (no conversion needed)
Weight data type: ``enterprises``

Filters applied:
  - ``unit`` = ``PC_ENT`` (Percentage of enterprises)
  - ``indic_is`` = ``E_DAOWN``

Note: This dataset provides NACE codes at broad section level only
(e.g. F). Only "F" (Construction) is extracted because other groupings
mix core and horizontal sub-codes and cannot be meaningfully decomposed.
"""

import pandas as pd

from extraction.base import BaseExtractor
from extraction.clients.eurostat import fetch_eurostat, find_time_col
from config.nace_codes import API_NACE_CODES_1LETTER


class DataAnalyticsExtractor(BaseExtractor):
    """Extractor for data analytics usage from Eurostat (isoc_eb_dan2).

    Fetches the percentage of enterprises where data analytics is
    performed by own employees.

    Class attributes:
        dataset_label: ``"Data Analytics"``
        weight_data_type: ``"enterprises"``
        unit: ``"Percentage of enterprises"``
        extra_id_columns: ``[]``
    """

    dataset_label = "Data Analytics"
    weight_data_type = "enterprises"
    unit = "Percentage of enterprises"
    extra_id_columns: list[str] = []

    def extract(self) -> pd.DataFrame:
        """Fetch data analytics data from Eurostat.

        Returns:
            DataFrame with columns: ``Country Code``, ``Country Name``,
            ``Year``, ``NACE Code``, ``Value``.

            Values are percentages of enterprises where data analytics
            is performed by own employees.
        """
        raw, geo_labels = fetch_eurostat(
            dataset_code="isoc_eb_dan2",
            params=[
                ("unit", "PC_ENT"),
                ("indic_is", "E_DAOWN"),
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
        print(f"  Data Analytics: {len(out)} rows extracted")
        return out
