"""
Extract: Artificial intelligence usage by NACE Rev. 2 activity and NUTS 2 region.

Dataset: ``isoc_r_eb_ain2``
Source:   Eurostat — Artificial intelligence by NACE Rev. 2 activity
          and NUTS 2 region
Indicators:
  - ``E_AI_TX`` — Don't use any AI technologies
  - ``E_AI_TANY`` — Using at least one AI technology
  - ``E_AI_TGE2`` — Using at least two AI technologies
  - ``E_AI_TGE3`` — Using at least three AI technologies
Unit:    Percentage of enterprises (no conversion needed)
Weight data type: ``enterprises``

Filters applied:
  - ``unit`` = ``PC_ENT`` (Percentage of enterprises)
  - ``indic_is`` = ``E_AI_TX``, ``E_AI_TANY``, ``E_AI_TGE2``, ``E_AI_TGE3``

Note: This dataset provides NACE codes at broad section level only
(e.g. F). Only "F" (Construction) is extracted because other groupings
mix core and horizontal sub-codes and cannot be meaningfully decomposed.
"""

import pandas as pd

from extraction.base import BaseExtractor
from extraction.clients.eurostat import fetch_eurostat, find_time_col
from config.nace_codes import API_NACE_CODES_1LETTER, AI_USAGE_LABELS


class AiUsageExtractor(BaseExtractor):
    """Extractor for AI technology usage from Eurostat (isoc_r_eb_ain2).

    Produces a long-format DataFrame with one row per
    (country, year, AI usage level, NACE code) combination.

    Class attributes:
        dataset_label: ``"AI Usage"``
        weight_data_type: ``"enterprises"``
        unit: ``"Percentage of enterprises"``
        extra_id_columns: ``["AI Usage"]``
    """

    dataset_label = "AI Usage"
    weight_data_type = "enterprises"
    unit = "Percentage of enterprises"
    extra_id_columns = ["AI Usage"]

    def extract(self) -> pd.DataFrame:
        """Fetch AI usage data from Eurostat.

        Returns:
            DataFrame with columns: ``Country Code``, ``Country Name``,
            ``Year``, ``AI Usage``, ``NACE Code``, ``Value``.

            Values are percentages of enterprises.
        """
        raw, geo_labels = fetch_eurostat(
            dataset_code="isoc_r_eb_ain2",
            params=[
                ("unit", "PC_ENT"),
                ("indic_is", "E_AI_TX"),
                ("indic_is", "E_AI_TANY"),
                ("indic_is", "E_AI_TGE2"),
                ("indic_is", "E_AI_TGE3"),
            ],
            nace_codes=API_NACE_CODES_1LETTER,
            aggregate_f=False,
        )

        time_col = find_time_col(raw)

        out = pd.DataFrame({
            "Country Code": raw["geo"],
            "Country Name": raw["geo"].map(lambda g: geo_labels.get(g, g)),
            "Year": raw[time_col].astype(int),
            "AI Usage": raw["indic_is"].map(AI_USAGE_LABELS),
            "NACE Code": raw["nace_r2"],
            "Value": raw["value"],
        })

        out.sort_values(
            ["Country Code", "Year", "NACE Code", "AI Usage"],
            inplace=True,
        )
        out.reset_index(drop=True, inplace=True)
        print(f"  AI Usage: {len(out)} rows extracted")
        return out
