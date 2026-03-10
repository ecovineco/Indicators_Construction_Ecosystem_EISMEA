"""
Extract: Internet access by NACE Rev. 2 activity and NUTS 2 region.

Dataset: ``isoc_r_ci_in_en2``
Source:   Eurostat — Internet access by NACE Rev. 2 activity and NUTS 2 region
Indicators:
  - ``E_IUSE_GT10`` — More than 10% of persons employed have internet access
  - ``E_IUSE_GT50`` — More than 50% of persons employed have internet access
Unit:    Percentage of enterprises (no conversion needed)
Weight data type: ``enterprises``

Filters applied:
  - ``unit`` = ``PC_ENT`` (Percentage of enterprises)
  - ``indic_is`` = ``E_IUSE_GT10``, ``E_IUSE_GT50``

Note: This dataset provides NACE codes at broad section level only
(e.g. F). Only "F" (Construction) is extracted because other groupings
mix core and horizontal sub-codes and cannot be meaningfully decomposed.
"""

import pandas as pd

from extraction.base import BaseExtractor
from extraction.clients.eurostat import fetch_eurostat, find_time_col
from config.nace_codes import API_NACE_CODES_1LETTER, INTERNET_ACCESS_LABELS


class InternetAccessExtractor(BaseExtractor):
    """Extractor for internet access from Eurostat (isoc_r_ci_in_en2).

    Produces a long-format DataFrame with one row per
    (country, year, internet access indicator, NACE code) combination.

    Class attributes:
        dataset_label: ``"Internet Access"``
        weight_data_type: ``"enterprises"``
        unit: ``"Percentage of enterprises"``
        extra_id_columns: ``["Internet Access"]``
    """

    dataset_label = "Internet Access"
    weight_data_type = "enterprises"
    unit = "Percentage of enterprises"
    extra_id_columns = ["Internet Access"]

    def extract(self) -> pd.DataFrame:
        """Fetch internet access data from Eurostat.

        Returns:
            DataFrame with columns: ``Country Code``, ``Country Name``,
            ``Year``, ``Internet Access``, ``NACE Code``, ``Value``.

            Values are percentages of enterprises.
        """
        raw, geo_labels = fetch_eurostat(
            dataset_code="isoc_r_ci_in_en2",
            params=[
                ("unit", "PC_ENT"),
                ("indic_is", "E_IUSE_GT10"),
                ("indic_is", "E_IUSE_GT50"),
            ],
            nace_codes=API_NACE_CODES_1LETTER,
            aggregate_f=False,
        )

        time_col = find_time_col(raw)

        out = pd.DataFrame({
            "Country Code": raw["geo"],
            "Country Name": raw["geo"].map(lambda g: geo_labels.get(g, g)),
            "Year": raw[time_col].astype(int),
            "Internet Access": raw["indic_is"].map(INTERNET_ACCESS_LABELS),
            "NACE Code": raw["nace_r2"],
            "Value": raw["value"],
        })

        out.sort_values(
            ["Country Code", "Year", "NACE Code", "Internet Access"],
            inplace=True,
        )
        out.reset_index(drop=True, inplace=True)
        print(f"  Internet Access: {len(out)} rows extracted")
        return out
