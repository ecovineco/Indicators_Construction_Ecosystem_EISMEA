"""
Extract: Net fixed assets per person employed (capital stock productivity).

Dataset: ``nama_10_cp_a21``
Source:   Eurostat — Capital stock based productivity indicators by industry
Indicator: ``na_item`` = ``NCS_EMP`` (Net fixed assets per employed person)
Asset:   ``asset10`` = ``N11N`` (Total fixed assets, net)
Unit:    Index (2020 = 100) — no conversion needed
Weight data type: ``other``

Note: This dataset only provides NACE codes at the A21 section level
(1-letter). Only "F" (Construction) is extracted because other letters
(C, E, M, N) mix core and horizontal sub-codes and cannot be meaningfully
decomposed.
"""

import pandas as pd

from extraction.base import BaseExtractor
from extraction.clients.eurostat import fetch_eurostat, find_time_col
from config.nace_codes import API_NACE_CODES_1LETTER


class InvestmentPerPersonExtractor(BaseExtractor):
    """Extractor for investment per person employed from Eurostat (nama_10_cp_a21).

    Produces a long-format DataFrame with one row per
    (country, year, NACE code) combination.

    Class attributes:
        dataset_label: ``"Investment per Person"``
        weight_data_type: ``"other"``
        unit: ``"Index (2020=100)"``
        extra_id_columns: ``[]``
    """

    dataset_label = "Investment per Person"
    weight_data_type = "other"
    unit = "Index (2020=100)"
    extra_id_columns: list[str] = []

    def extract(self) -> pd.DataFrame:
        """Fetch investment per person employed data from Eurostat.

        Returns:
            DataFrame with columns: ``Country Code``, ``Country Name``,
            ``Year``, ``NACE Code``, ``Value``.

            Values are index numbers (2020 = 100).
        """
        raw, geo_labels = fetch_eurostat(
            dataset_code="nama_10_cp_a21",
            params=[
                ("na_item", "NCS_EMP"),
                ("asset10", "N11N"),
                ("unit", "I20"),
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
        print(f"  Investment per Person: {len(out)} rows extracted")
        return out
