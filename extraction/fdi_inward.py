"""
Extract: Foreign direct investment (inward) in the reporting economy.

Dataset: ``bop_fdi6_pos``
Source:   Eurostat — EU direct investment positions by country, ultimate and
          immediate counterpart and economic activity (BPM6)
Unit:     Million euro (no conversion needed)
Weight data type: ``other``

Filters applied:
  - ``stk_flow`` = ``NI`` (Net FDI inward)
  - ``fdi_item`` = ``DI__D__F`` (Direct investment in the reporting economy)
  - ``currency`` = ``MIO_EUR`` (Million euro)
  - ``counterp`` = ``IMM`` (Immediate counterpart)
  - ``entity`` = ``TOTAL`` (All entities)
  - ``partner`` = ``WRL_REST`` (Rest of the world — aggregate)

Note: This dataset only provides NACE codes at the section level.
Only "F" (Construction) is extracted because other letters (C, E, M, N)
mix core and horizontal sub-codes and cannot be meaningfully decomposed.
"""

import pandas as pd

from extraction.base import BaseExtractor
from extraction.clients.eurostat import fetch_eurostat, find_time_col
from config.nace_codes import API_NACE_CODES_1LETTER


class FdiInwardExtractor(BaseExtractor):
    """Extractor for inward FDI from Eurostat (bop_fdi6_pos).

    Produces a long-format DataFrame with one row per
    (country, year, NACE code) combination.

    Class attributes:
        dataset_label: ``"FDI Inward"``
        weight_data_type: ``"other"``
        unit: ``"EUR million"``
        extra_id_columns: ``[]``
    """

    dataset_label = "FDI Inward"
    weight_data_type = "other"
    unit = "EUR million"
    extra_id_columns: list[str] = []

    def extract(self) -> pd.DataFrame:
        """Fetch inward FDI data from Eurostat.

        Returns:
            DataFrame with columns: ``Country Code``, ``Country Name``,
            ``Year``, ``NACE Code``, ``Value``.

            Values are in EUR million (source unit unchanged).
        """
        raw, geo_labels = fetch_eurostat(
            dataset_code="bop_fdi6_pos",
            params=[
                ("stk_flow", "NI"),
                ("fdi_item", "DI__D__F"),
                ("currency", "MIO_EUR"),
                ("counterp", "IMM"),
                ("entity", "TOTAL"),
                ("partner", "WRL_REST"),
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
        print(f"  FDI Inward: {len(out)} rows extracted")
        return out
