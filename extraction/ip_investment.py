"""
Extract: Gross capital formation in intellectual property products.

Dataset: ``nama_10_a64_p5``
Source:   Eurostat — Gross capital formation by industry and asset type
Indicator: ``na_item`` = ``P5G`` (Gross capital formation)
Asset:   ``asset10`` = ``N117G`` (Intellectual property products, gross)
Unit:    EUR million at current prices (no conversion needed)
Weight data type: ``other``

Note: Despite the A64 dataset name, NACE codes are only available at the
section level (1-letter). Only "F" (Construction) is extracted because
other letters (C, E, M, N) mix core and horizontal sub-codes and cannot
be meaningfully decomposed.
"""

import pandas as pd

from extraction.base import BaseExtractor
from extraction.clients.eurostat import fetch_eurostat, find_time_col
from config.nace_codes import API_NACE_CODES_1LETTER


class IpInvestmentExtractor(BaseExtractor):
    """Extractor for IP investment from Eurostat (nama_10_a64_p5).

    Produces a long-format DataFrame with one row per
    (country, year, NACE code) combination.

    Class attributes:
        dataset_label: ``"IP Investment"``
        weight_data_type: ``"other"``
        unit: ``"EUR million"``
        extra_id_columns: ``[]``
    """

    dataset_label = "IP Investment"
    weight_data_type = "other"
    unit = "EUR million"
    extra_id_columns: list[str] = []

    def extract(self) -> pd.DataFrame:
        """Fetch intellectual property investment data from Eurostat.

        Returns:
            DataFrame with columns: ``Country Code``, ``Country Name``,
            ``Year``, ``NACE Code``, ``Value``.

            Values are in EUR million at current prices.
        """
        raw, geo_labels = fetch_eurostat(
            dataset_code="nama_10_a64_p5",
            params=[
                ("na_item", "P5G"),
                ("asset10", "N117G"),
                ("unit", "CP_MEUR"),
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
        print(f"  IP Investment: {len(out)} rows extracted")
        return out
