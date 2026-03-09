"""
Extract: Use of computers and the internet by employees, by NACE activity.

Dataset: ``isoc_r_ci_cm_pn2``
Source:   Eurostat — Use of computers and the internet by employees,
          by NACE Rev. 2 activity and NUTS 2 region
Indicator: ``indic_is`` = ``P_IUSE`` (Persons with internet access for
           business purposes)
Unit:    Percentage of total employment (no conversion needed)
Weight data type: ``other``

Filters applied:
  - ``unit`` = ``PC_EMP`` (Percentage of total employment)
  - ``indic_is`` = ``P_IUSE`` (Internet use)
  - ``size_emp`` = ``GE10`` (10 or more employees)

Note: This dataset provides NACE codes at broad section-group level only
(e.g. C-F, F, G). Only "F" (Construction) is extracted because other
groupings mix core and horizontal sub-codes and cannot be meaningfully
decomposed.
"""

import pandas as pd

from extraction.base import BaseExtractor
from extraction.clients.eurostat import fetch_eurostat, find_time_col
from config.nace_codes import API_NACE_CODES_1LETTER


class IctUsageExtractor(BaseExtractor):
    """Extractor for ICT usage by employees from Eurostat (isoc_r_ci_cm_pn2).

    Produces a long-format DataFrame with one row per
    (country, year, NACE code) combination.

    Class attributes:
        dataset_label: ``"ICT Usage"``
        weight_data_type: ``"other"``
        unit: ``"Percentage"``
        extra_id_columns: ``[]``
    """

    dataset_label = "ICT Usage"
    weight_data_type = "other"
    unit = "Percentage"
    extra_id_columns: list[str] = []

    def extract(self) -> pd.DataFrame:
        """Fetch ICT usage data from Eurostat.

        Returns:
            DataFrame with columns: ``Country Code``, ``Country Name``,
            ``Year``, ``NACE Code``, ``Value``.

            Values are percentages of total employment (enterprises with
            10+ employees).
        """
        raw, geo_labels = fetch_eurostat(
            dataset_code="isoc_r_ci_cm_pn2",
            params=[
                ("unit", "PC_EMP"),
                ("indic_is", "P_IUSE"),
                ("size_emp", "GE10"),
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
        print(f"  ICT Usage: {len(out)} rows extracted")
        return out
